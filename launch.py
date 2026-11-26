#!/usr/bin/env python3
"""Starts a ServEval server or client as one process, one per interface on its memory domain, or several copies,
and merges the reports of several client processes, which split every connection count between them.

Programs named like `serv-eval-axum-server` or `fastapi_server.py` are servers, others clients, and servers started
with `--copies` are upstreams. A server takes its cores from `SERVEVAL_SERVER_CPU_LISTS`, like `8-9/104-105`, one
list per interface in `SERVEVAL_INTERFACES` order: `per-domain` gives process i list i, `one` gives the union.
Without the variable, `--server-cores-per-network-interface N` derives those lists: the first N cores of each
interface's memory domain after its `--housekeeping-cores-per-memory-domain`, within this launcher's affinity mask,
leaving out every core a network device's interrupt is pinned to. Clients, upstreams, and servers without either take
an even share of this launcher's cores and memory.

With `--server COMMAND`, the launcher runs a pair: for every `--server-cores-per-network-interface` value and every
connection count of the client's sweep, it starts the server through this launcher, on `--server-host` over ssh or on
this box, runs the client after `--` once per `--client-passes` entry against it, and stops it, so each server is sized
for one count and placed on one budget.

Whenever `sudo -n systemd-run` works, every process is a transient unit of `serveval.slice` running as the calling
user, with `AllowedCPUs`, `MemoryMax`, its limits, and a NUMA binding when its cores sit on one memory domain, so
`systemctl stop serveval.slice` ends every one of them; otherwise plain processes pinned by affinity alone.
"""

import argparse
import json
import os
import pathlib
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

import arguments
import results
import stats

FORWARDED = ("PATH", "HOME", "PYTHONPATH", "VIRTUAL_ENV", "OPENSSL_CONF")
SLICE = "serveval"
# A stop gets this long before SIGKILL, unit or plain process, so a server that ignores its stop signal cannot hold its
# port into the next pair.
STOP_SECONDS = 10
# How long one TCP connection attempt to a server may take while waiting for it to listen.
CONNECT_SECONDS = 1


def parse_cpus(text: str) -> list[int]:
    cpus = []
    for part in filter(None, text.strip().split(",")):
        first, _, last = part.partition("-")
        cpus += range(int(first), int(last or first) + 1)
    return cpus


def spell_cpus(cpus: list[int]) -> str:
    ranges, start = [], None
    for index, cpu in enumerate(cpus):
        start = cpu if start is None else start
        if index + 1 == len(cpus) or cpus[index + 1] != cpu + 1:
            ranges.append(f"{start}-{cpu}" if cpu != start else f"{cpu}")
            start = None
    return ",".join(ranges)


def node_cpus() -> dict[int, set[int]]:
    """Every memory domain's cores, from the kernel's node lists; empty where the kernel exposes none."""
    nodes = pathlib.Path("/sys/devices/system/node")
    found = {}
    for path in nodes.glob("node[0-9]*/cpulist"):
        found[int(path.parent.name.removeprefix("node"))] = set(parse_cpus(path.read_text()))
    return found


def domain_holding(cpus: list[int]) -> int:
    """The one memory domain holding every core of `cpus`, or -1 when they span several or the kernel cannot say."""
    holding = {node for node, on_node in node_cpus().items() if on_node & set(cpus)}
    return holding.pop() if len(holding) == 1 else -1


def domains_of(interfaces: list[arguments.Interface]) -> list[int]:
    """The memory domain of each interface's device from UCall's binding, or -1 for every one without it."""
    try:
        import ucall
    except ImportError:
        print("- Domains: -1 for every interface, as the ucall Python binding is not importable", file=sys.stderr)
        return [-1] * len(interfaces)
    listeners = []
    for interface in interfaces:
        address = interface.addresses[0]
        device = {"device_name": interface.device} if interface.device else {}
        listeners.append(ucall.Interface(f"[{address}]:0" if ":" in address else f"{address}:0", **device))
    return list(ucall.Server(*listeners).memory_domains)


def domain_cpus(domain: int, allowed: list[int]) -> list[int]:
    """The allowed cores of `domain`, or every allowed core for -1."""
    path = pathlib.Path(f"/sys/devices/system/node/node{domain}/cpulist")
    if domain < 0 or not path.exists():
        return allowed
    on_node = set(parse_cpus(path.read_text()))
    return [cpu for cpu in allowed if cpu in on_node] or allowed


def network_device_interrupt_cores() -> set[int]:
    """Every core an interrupt of a network device is pinned to, from each device's MSI interrupts in sysfs and their
    `/proc/irq/<irq>/smp_affinity_list`."""
    cores = set()
    for interrupt in pathlib.Path("/sys/class/net").glob("*/device/msi_irqs/*"):
        affinity = pathlib.Path(f"/proc/irq/{interrupt.name}/smp_affinity_list")
        if affinity.exists():
            cores |= set(parse_cpus(affinity.read_text()))
    return cores


def devices_of(interface: arguments.Interface, targets: list[str]) -> list[str]:
    """The device an interface names or holds its address; a wildcard address, as LitServe binds, stands for the
    devices holding the targets' addresses, which are this box's when the pair launches its server."""
    address = interface.addresses[0]
    if interface.device:
        return [interface.device]
    held = [arguments.device_holding(target) for target in targets] if address in ("0.0.0.0", "::") else []
    devices = [device for device in held if device] or [arguments.device_holding(address)]
    if None in devices:
        arguments.exit(f"no device holds {address}; name it as DEVICE=ADDRESS")
    return devices


def derived_cpu_lists(interfaces, targets: list[str], cores_per_interface: int, housekeeping: int) -> str:
    """Like `8-11/104-107`: per device of the interfaces, the next `cores_per_interface` cores of its memory domain
    that are past the domain's first `housekeeping` cores, inside this launcher's affinity mask, and pinned to no
    network device's interrupt. A device without a memory domain draws from the mask alone."""
    allowed = sorted(os.sched_getaffinity(0))
    interrupt_cores, taken, lists = network_device_interrupt_cores(), set(), []
    for device in (device for interface in interfaces for device in devices_of(interface, targets)):
        node = pathlib.Path(f"/sys/class/net/{device}/device/numa_node")
        domain = int(node.read_text()) if node.exists() else -1
        cores = sorted(node_cpus().get(domain, allowed))
        usable = set(allowed) - interrupt_cores - taken
        candidates = [core for core in cores[housekeeping:] if core in usable]
        if len(candidates) < cores_per_interface:
            arguments.exit(
                f"{device} on memory domain {domain} has {len(candidates)} cores past {housekeeping} housekeeping "
                f"cores, inside the mask and free of interrupts, fewer than {cores_per_interface}"
            )
        lists.append(candidates[:cores_per_interface])
        taken |= set(lists[-1])
    return "/".join(spell_cpus(cpus) for cpus in lists)


def listed_placements(layout: str, interfaces, domains, port: int, server_cpu_lists: str) -> list[dict]:
    """Each server process's interfaces, port, domain and cores from the per-interface lists, like `8-9/104-105`."""
    lists = [parse_cpus(text) for text in server_cpu_lists.split("/")]
    if not all(lists):
        arguments.exit(f'SERVEVAL_SERVER_CPU_LISTS="{server_cpu_lists}" names an empty list')
    if layout == "per-domain" and len(lists) != len(interfaces):
        arguments.exit(f"{len(lists)} CPU lists for {len(interfaces)} interfaces; name one list per interface")
    # A `one` server may listen on fewer interfaces than the lists name, like LitServe on 0.0.0.0, and still gets
    # every interface's cores, as the servers of the other layouts do together.
    pairs = [([index], lists[index]) for index in range(len(interfaces))]
    if layout == "one":
        pairs = [(list(range(len(interfaces))), sorted({cpu for cpus in lists for cpu in cpus}))]
    placed = []
    for group, cpus in pairs:
        domain = domain_holding(cpus)
        named = {domains[index] for index in group}
        if domain >= 0 and named != {-1} and named != {domain}:
            arguments.exit(f"cores {spell_cpus(cpus)} sit on memory domain {domain}, not on their interface's {named}")
        environment = {
            "SERVEVAL_INTERFACES": ",".join(str(interfaces[index]) for index in group),
            "SERVEVAL_PORT": str(port),
        }
        placed.append({"environment": environment, "domain": domain, "cpus": cpus, "interfaces": group})
    return placed


def placements(layout: str, copies: int, interfaces, targets, domains, port: int) -> list[dict]:
    """Each process's interfaces, targets, port, domain and cores: an even split of the cores it shares."""
    allowed = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else list(range(os.cpu_count()))
    groups = [(list(range(len(interfaces))), -1 if len(set(domains)) != 1 else domains[0])]
    if layout == "per-domain":
        groups = [([index], domain) for index, domain in enumerate(domains)]
    processes = [(group, domain, copy) for group, domain in groups for copy in range(copies)]
    placed = []
    for group, domain, copy in processes:
        sharing = [process for process in processes if process[1] == domain]
        cores = domain_cpus(domain, allowed)
        share, position = len(cores) // len(sharing), sharing.index((group, domain, copy))
        if share == 0:
            arguments.exit(f"{len(sharing)} processes share the {len(cores)} cores of domain {domain}; start fewer")
        environment = {
            "SERVEVAL_INTERFACES": ",".join(str(interfaces[index]) for index in group),
            "SERVEVAL_PORT": str(port + copy),
        }
        if targets:
            environment["SERVEVAL_TARGETS"] = ",".join(targets[index % len(targets)] for index in group)
        cpus = cores[position * share : (position + 1) * share]
        # Bare addresses leave the interface's domain unknown, while the cores still name one to keep memory on.
        domain = domain if domain >= 0 else domain_holding(cpus)
        placed.append({"environment": environment, "domain": domain, "cpus": cpus, "interfaces": group})
    return placed


def connection_shares(command: list[str], environment: dict, index: int, count: int) -> tuple[list[str], dict]:
    """`command` and `environment` with every connection count replaced by client process `index`'s share of `count`,
    so the processes together open the totals asked for, the first ones taking the remainder."""

    def shares(text: str) -> str:
        totals = arguments.list_of(arguments.parse_count)(text)
        if min(totals) < count:
            arguments.exit(f"{count} client processes cannot split {min(totals)} connections; start fewer")
        return ",".join(str(arguments.share(total, index, count)) for total in totals)

    command, given = list(command), False
    for position, part in enumerate(command):
        if part == "--connections" and position + 1 < len(command):
            command[position + 1], given = shares(command[position + 1]), True
        elif part.startswith("--connections="):
            command[position], given = "--connections=" + shares(part.partition("=")[2]), True
    if given:
        return command, environment
    return command, {**environment, "SERVEVAL_CONNECTIONS": shares(environment.get("SERVEVAL_CONNECTIONS") or "64")}


def systemd_usable() -> bool:
    """Whether this user may start transient units without a password prompt."""
    try:
        probe = subprocess.run(["sudo", "-n", "systemd-run", "--version"], capture_output=True, check=False)
    except FileNotFoundError:
        return False
    return probe.returncode == 0


def available_memory_bytes() -> int:
    """The memory the kernel reports available to new work, from `MemAvailable` in /proc/meminfo."""
    for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return 0


def unit_command(name: str, placement: dict, memory: int | None, environment: dict, command: list[str]) -> list[str]:
    """`command` as a transient unit of `serveval.slice`, run as the calling user, with its cores, limits and node."""
    files = int(pathlib.Path("/proc/sys/fs/nr_open").read_text())
    properties = [
        f"AllowedCPUs={spell_cpus(placement['cpus'])}",
        f"LimitNOFILE={files}",
        "LimitMEMLOCK=infinity",
        f"TimeoutStopSec={STOP_SECONDS}s",
    ]
    if memory:
        properties.append(f"MemoryMax={memory}")
    if placement["domain"] >= 0:  # a `one` server spanning both domains binds no node and places its own threads
        properties += ["NUMAPolicy=bind", f"NUMAMask={placement['domain']}"]
    identity = [f"--uid={os.getuid()}", f"--gid={os.getgid()}", f"--slice={SLICE}"]
    setenv = [f"--setenv={key}={value}" for key, value in environment.items()]
    # The manager resolves a relative program against its own directory and PATH, not this launcher's.
    program = shutil.which(command[0]) or command[0]
    unit = ["sudo", "-n", "systemd-run", "--wait", "--pipe", "--collect", "--same-dir", f"--unit={name}", *identity]
    return [*unit, *(f"-p{property}" for property in properties), *setenv, os.path.abspath(program), *command[1:]]


def hand_memory_peak(unit: str, child: subprocess.Popen, seconds: float = 300) -> None:
    """Gives the calling user the unit's `memory.peak`, which systemd leaves to root, so the server can reset it
    through its own descriptor and report each window's peak; prints why when it cannot."""
    deadline = time.monotonic() + seconds
    while True:
        shown = subprocess.run(
            ["systemctl", "show", "--property=ControlGroup", "--value", f"{unit}.service"],
            capture_output=True, text=True, check=False,
        )  # fmt: skip
        group = shown.stdout.strip()
        if group:
            break
        if child.poll() is not None or time.monotonic() > deadline:
            print(f"- {unit}: no control group, so its memory peak stays unreset", file=sys.stderr)
            return
        time.sleep(0.05)
    peak = f"/sys/fs/cgroup{group}/memory.peak"
    owner = f"{os.getuid()}:{os.getgid()}"
    if subprocess.run(["sudo", "-n", "chown", owner, peak], check=False).returncode != 0:
        print(f"- {unit}: cannot hand {peak} to {owner}, so its memory peak stays unreset", file=sys.stderr)


def wait_ready(addresses: list[tuple[str, int]], children: list[subprocess.Popen], seconds: float = 300):
    """The first address that accepted no TCP connection before a child died or time ran out, None once all did."""
    deadline = time.monotonic() + seconds
    for address in addresses:
        while True:
            try:
                socket.create_connection(address, timeout=CONNECT_SECONDS).close()
                break
            except OSError:
                if any(child.poll() is not None for child in children) or time.monotonic() > deadline:
                    return address
                time.sleep(0.05)
    return None


def accepting(address: tuple[str, int]) -> bool:
    try:
        socket.create_connection(address, timeout=CONNECT_SECONDS).close()
        return True
    except OSError:
        return False


def with_flag(command: list[str], flag: str, value: str) -> list[str]:
    """`command` with `flag` set to `value`, replacing it where given as `--flag VALUE` or `--flag=VALUE`."""
    replaced, skip = [], False
    for position, part in enumerate(command):
        if skip:
            skip = False
        elif part == flag and position + 1 < len(command):
            skip = True
        elif not part.startswith(flag + "="):
            replaced.append(part)
    return [*replaced, flag, value]


# Flags of the pair driver, which no launch it starts reads, so none of them starts another pair.
PAIR_VARIABLES = ("SERVEVAL_SERVER", "SERVEVAL_SERVER_HOST", "SERVEVAL_SERVER_INTERFACES", "SERVEVAL_CLIENT_PASSES")


def start_server(options, layout: str, port: int, environment: dict) -> subprocess.Popen:
    """The server's launcher, on `--server-host` over ssh or on this box, from this launcher's directory and Python,
    which the server box must share; it stops its server once its standard input closes, as when this driver ends,
    and ends with the launcher."""
    launch = [sys.executable, str(pathlib.Path(__file__).resolve()), "--layout", layout, "--port", str(port), "--"]
    assignments = " ".join(f"{key}={shlex.quote(value)}" for key, value in sorted(environment.items()))
    # The soft descriptor limit rises to the hard one, for plain processes; units set their own.
    script = (
        f'cd {shlex.quote(str(pathlib.Path(__file__).resolve().parent))} || exit 1; ulimit -n "$(ulimit -Hn)"; '
        f"env {assignments} {shlex.join(launch + shlex.split(options.server))} < /dev/null & server=$!; "
        # A background list reads /dev/null unless redirected, so the watcher reads the input through descriptor 3;
        # the shell waits for the launcher alone, so it ends, and shows the launch failed, when the launcher does.
        "exec 3<&0; { read -r _ <&3; kill -TERM $server; } > /dev/null 2>&1 & wait $server"
    )
    line = ["ssh", "-o", "BatchMode=yes", options.server_host, script] if options.server_host else ["sh", "-c", script]
    local = {key: value for key, value in os.environ.items() if not key.startswith("SERVEVAL_")}
    server = subprocess.Popen(line, env=local, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True)  # fmt: skip
    threading.Thread(target=lambda: [print(f"server: {output}", end="", file=sys.stderr) for output in server.stdout],
                     daemon=True).start()  # fmt: skip
    return server


def stop_server(server: subprocess.Popen, seconds: float = 2 * STOP_SECONDS) -> None:
    """Closes the server launcher's input, which stops its units or processes, and waits for it to finish: its stop
    escalates to SIGKILL after `STOP_SECONDS`, and the launcher and ssh get as long again to end."""
    server.stdin.close()
    try:
        server.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        print(f"- Server launcher still running after {seconds:.0f} s; killing it", file=sys.stderr)
        server.kill()
        server.wait()


def run_passes(layout: str, port: int, client: list[str], passes: list[list[str]], environment: dict, step: str) -> int:
    """Launches the client once per pass, its extra flags appended; returns how many launches failed."""
    failures = 0
    for extra in passes:
        launch = [sys.executable, str(pathlib.Path(__file__).resolve()), "--layout", layout, "--port", str(port)]
        code = subprocess.run([*launch, "--", *client, *extra], env=environment, check=False).returncode
        if code != 0:
            print(f"- {step}: client pass `{shlex.join(extra)}` exited {code}", file=sys.stderr)
            failures += 1
    return failures


def run_pairs(options, client: list[str]) -> int:
    """Runs the client against a fresh server per `--server-cores-per-network-interface` value and connection count,
    once per client pass; returns how many steps failed, each named on stderr and skipped."""
    environment = {key: value for key, value in os.environ.items() if key.startswith("SERVEVAL_")}
    for variable in (*PAIR_VARIABLES, "SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE", "SERVEVAL_CONNECTIONS"):
        environment.pop(variable, None)
    # Explicit lists pin the server's cores, as when interrupts move onto them, and one cores value then only names
    # the budget the rows record; several values would all run on the same pinned cores.
    if len(options.server_cores_per_network_interface or []) > 1 and "SERVEVAL_SERVER_CPU_LISTS" in environment:
        arguments.exit("SERVEVAL_SERVER_CPU_LISTS pins the server's cores; sweep one cores value with it")
    housekeeping = str(options.housekeeping_cores_per_memory_domain)
    environment["SERVEVAL_HOUSEKEEPING_CORES_PER_MEMORY_DOMAIN"] = housekeeping
    # The client's own flags and `SERVEVAL_*` defaults, as its parser reads them, name the counts, targets, port,
    # interfaces and body sizes.
    first_flag = next((index for index, word in enumerate(client) if word.startswith("-")), len(client))
    settings, _ = arguments.client_parser(__doc__).parse_known_args(client[first_flag:])
    port = options.port or settings.port
    addresses = [(target, port) for target in settings.targets]
    server_interfaces = options.server_interfaces or ",".join(map(str, settings.interfaces))
    if {"context", "download"} & set(settings.workloads):
        # Both sides take the client's sweep, so a server never prepares fewer bytes than the client asks for.
        environment["SERVEVAL_BODY_BYTES"] = ",".join(map(str, arguments.body_bytes(settings)))
    passes = [shlex.split(flags) for flags in options.client_passes.split(";")]
    local = {key: value for key, value in os.environ.items() if not key.startswith("SERVEVAL_")}
    sweeping = {}
    if options.client:
        # Each client launch against a server sweeps these clients, as `sweep_clients` does.
        sweeping["SERVEVAL_CLIENT"] = ",".join(shlex.join(program) for program in options.client)
    failures = 0
    for cores in options.server_cores_per_network_interface or [None]:
        for count in settings.connections:
            budget = {"SERVEVAL_CONNECTIONS": str(count)}
            if cores is not None:
                budget["SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE"] = str(cores)
            step = f"{count} connections" + (f", {cores} server cores per network interface" if cores else "")
            if any(accepting(address) for address in addresses):
                print(f"- {step}: a target already accepts on port {port} before its server started", file=sys.stderr)
                failures += 1
                continue
            server_environment = {**environment, **budget, "SERVEVAL_INTERFACES": server_interfaces}
            server = start_server(options, options.layout, port, server_environment)
            unready = wait_ready(addresses, [server])
            if unready is not None:
                print(f"- {step}: {unready[0]} port {unready[1]} never accepted a connection", file=sys.stderr)
                stop_server(server)
                failures += 1
                continue
            print(f"- {step}: server ready", file=sys.stderr, flush=True)
            client_environment = {**local, **environment, **budget, **sweeping}
            client_command = with_flag(client, "--connections", str(count))
            failures += run_passes(options.layout, port, client_command, passes, client_environment, step)
            stop_server(server)
    return failures


def relay(name: str, child: subprocess.Popen, reports: list[dict]) -> None:
    """Prints a child's lines under its name, keeping the JSON reports."""
    for line in child.stdout:
        if line.startswith("{"):
            reports.append(json.loads(line))
            print(line, end="", flush=True)
        else:
            print(f"{name}: {line}", end="", flush=True)


def merge_reports(reports: list[dict], placed: list[dict], domains: list[int], output: str) -> None:
    """Writes one report per configuration measured by several processes, and removes the per-process files. Every
    process sweeps the same runs in order, on its own share of the cores and connections, so the n-th report of each
    joins the n-th of the others."""
    groups: dict[str, list[dict]] = {}
    seen: dict[int, int] = {}
    for report in reports:
        process = report["config"]["process"]
        seen[process] = seen.get(process, -1) + 1
        shared = dict(report["config"], process=None, threads=None, connections=None, in_flight=None, run=seen[process])
        groups.setdefault(json.dumps(shared), []).append(report)
    for group in groups.values():
        used = {interface for report in group for interface in placed[report["config"]["process"]]["interfaces"]}
        merged = results.merge(group, [domains[interface] for interface in sorted(used)])
        print(json.dumps(merged, separators=(",", ":")), flush=True)
        for report in group:
            results.report_path(output, report).unlink(missing_ok=True)
        print(f"- Merged {len(group)} processes into {results.write_report(output, merged)}", file=sys.stderr)


def sweep_clients(options: argparse.Namespace, command: list[str]) -> int:
    """Launches the client once per `--client` program, one after another against the server already listening,
    each in place of `command`'s own program, the words before its first flag, with the same flags. Each launch
    writes into the same output, where rows tell the clients apart by the `client` they name. A stop signal ends
    the running launch and the sweep; otherwise every launch runs, and any failure makes the result 1."""
    first_flag = next((index for index, word in enumerate(command) if word.startswith("-")), len(command))
    flags = command[first_flag:]
    for program in [command[:first_flag], *options.client]:
        role = pathlib.Path(next((word for word in program if word.endswith(".py")), program[0])).stem
        if "_server" in role.removeprefix("serv-eval-").replace("-", "_"):
            arguments.exit(f"--client sweeps clients, and {shlex.join(program)} is a server")
    if options.smoke or options.copies > 1:
        arguments.exit("--client launches clients, so it takes neither --smoke nor --copies")
    launcher = [sys.executable, os.path.abspath(__file__), "--layout", options.layout]
    if options.port:
        launcher += ["--port", str(options.port)]
    # The launches below read this variable too, and each must launch its one client, not sweep again.
    environment = {key: value for key, value in os.environ.items() if key != "SERVEVAL_CLIENT"}
    running: list[subprocess.Popen] = []
    stopped = threading.Event()

    def stop(*_) -> None:
        stopped.set()
        for child in running:
            child.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, stop)
    codes = []
    for program in options.client:
        if stopped.is_set():
            break
        print(f"- Client: {shlex.join(program)}", file=sys.stderr, flush=True)
        running[:] = [subprocess.Popen([*launcher, "--", *program, *flags], env=environment)]
        codes.append(running[0].wait())
    return 1 if stopped.is_set() or any(codes) else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.partition("\n")[0])
    parser.add_argument("--layout", choices=("one", "per-domain"), default="one", help="Processes per interface set.")
    parser.add_argument("--copies", type=arguments.parse_count, default=1, help="Copies of a server on adjacent ports.")
    parser.add_argument("--port", type=arguments.parse_port, default=None, help="First port, SERVEVAL_PORT by default.")
    parser.add_argument("--smoke", metavar="CLIENT", help="Run CLIENT for a 200ms steady tool exchange, then stop.")
    arguments.add_flag(
        parser,
        "--client",
        None,
        type=arguments.list_of(shlex.split),
        metavar="<PROGRAM,...>",
        help="Clients to sweep against one running server, one launch each, every one replacing the client program "
        "before the flags after --, like `target/release/serv-eval-hyper-client,.venv/bin/python httpx_client.py`.",
    )
    arguments.add_flag(
        parser,
        "--server-cores-per-network-interface",
        None,
        type=arguments.list_of(arguments.parse_count),
        help="Server cores per network interface: one value places a server, several are swept by --server.",
    )
    arguments.add_flag(
        parser,
        "--housekeeping-cores-per-memory-domain",
        "0",
        type=arguments.parse_count_or_zero,
        help="First cores of each memory domain left to the kernel, sshd and recorders, never serving.",
    )
    arguments.add_flag(parser, "--server", None, help="A server command, making this launch a pair with the client.")
    arguments.add_flag(parser, "--server-host", None, help="The ssh destination the pair's server runs on.")
    arguments.add_flag(
        parser, "--server-interfaces", None, help="The pair server's SERVEVAL_INTERFACES, the client's if unset."
    )
    arguments.add_flag(
        parser,
        "--client-passes",
        "",
        help="Extra client flags per pass, passes separated by ;, each run against the same server launch.",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The binary and its flags, after --.")
    options = parser.parse_args()
    command = options.command[1:] if options.command[:1] == ["--"] else options.command
    if not command:
        arguments.exit("Name the binary after --, like ./launch.py -- target/release/serv-eval-axum-server")
    options.client = [program for program in options.client or [] if program]
    if options.server:
        sys.exit(1 if run_pairs(options, command) else 0)
    if options.client:
        sys.exit(sweep_clients(options, command))
    environment = {key: value for key, value in os.environ.items() if key.startswith("SERVEVAL_") or key in FORWARDED}
    interfaces = arguments.list_of(arguments.parse_interface)(os.environ.get("SERVEVAL_INTERFACES", "127.0.0.1"))
    targets = os.environ.get("SERVEVAL_TARGETS", "127.0.0.1").split(",")
    port = options.port or int(os.environ.get("SERVEVAL_PORT", "8545"))
    program = pathlib.Path(next((part for part in command if part.endswith(".py")), command[0])).stem
    role = program.removeprefix("serv-eval-").replace("-", "_")
    server = "_server" in role
    name = role.partition("_")[0]
    kind = "client" if not server else "upstream" if options.copies > 1 else "server"
    domains = domains_of(interfaces)
    server_cpu_lists = os.environ.get("SERVEVAL_SERVER_CPU_LISTS", "") if kind == "server" else ""
    cores_per_interface = options.server_cores_per_network_interface or []
    if len(cores_per_interface) > 1 and kind in ("server", "client"):
        arguments.exit(
            f"--server-cores-per-network-interface takes one value for a {kind}; a --server pair sweeps them"
        )
    if kind == "server" and cores_per_interface and not server_cpu_lists:
        housekeeping = options.housekeeping_cores_per_memory_domain
        server_cpu_lists = derived_cpu_lists(interfaces, targets, cores_per_interface[0], housekeeping)
        print(f"- Server CPU lists: {server_cpu_lists}, {cores_per_interface[0]} per network interface past "
              f"{housekeeping} housekeeping cores per memory domain", file=sys.stderr)  # fmt: skip
    if server_cpu_lists:
        placed = listed_placements(options.layout, interfaces, domains, port, server_cpu_lists)
    else:
        placed = placements(options.layout, options.copies, interfaces, [] if server else targets, domains, port)
    budget = stats.unit_stats()
    memory = budget["memory_limit"] // len(placed) if budget["memory_limit"] else None
    if memory is None and not server:
        # A client sweeping millions of calls in flight can outgrow the box, as tonic did at 65,536 connections
        # times 100 streams; its units split what is available, so the kernel ends that client, not the box.
        memory = available_memory_bytes() // len(placed)
    spelled = ", ".join(f"{interface} (domain {domain})" for interface, domain in zip(interfaces, domains))
    print(f"- Interfaces: {spelled} · Layout: {len(placed)} processes", file=sys.stderr)
    for index, placement in enumerate(placed):
        domain = placement["domain"]
        print(f"- Process {index}: cores {spell_cpus(placement['cpus'])}, memory domain {domain}", file=sys.stderr)
    for limit, value in results.kernel_limits().items():
        if value is not None:
            print(f"- {limit}: {value}", file=sys.stderr)
    if not server:
        unready = wait_ready([(target, port) for target in targets], [])
        if unready is not None:
            arguments.exit(f"{unready[0]} port {unready[1]} never accepted a connection")
        environment["SERVEVAL_BARRIER"] = tempfile.mkdtemp(prefix="serveval-barrier-")
    systemd = systemd_usable()
    if systemd:
        print(f"- Processes: units of {SLICE}.slice, as uid {os.getuid()}", file=sys.stderr)
    else:
        print("- Processes: plain, pinned by affinity, since `sudo -n systemd-run` is unavailable", file=sys.stderr)
    children, threads, reports = [], [], []
    for index, placement in enumerate(placed):
        # The role and port keep a client's units apart from its own upstreams' and from a server on the same box.
        unit = f"serveval-{kind}-{name}-{port}-{index}"
        child_environment, own = {**environment, **placement["environment"]}, command
        if not server:
            child_environment["SERVEVAL_PROCESS"] = f"{index}/{len(placed)}"
            if len(placed) > 1:
                own, child_environment = connection_shares(command, child_environment, index, len(placed))
        line = unit_command(unit, placement, memory, child_environment, own) if systemd else own
        pinning = not systemd and hasattr(os, "sched_setaffinity")
        if pinning:
            # The child inherits the mask of the thread that starts it.
            allowed = os.sched_getaffinity(0)
            os.sched_setaffinity(0, placement["cpus"])
        environment_now = {**os.environ, **child_environment}
        # A plain process leads its own process group, so a stop reaches the workers it forks, as a unit's does.
        child = subprocess.Popen(
            line, env=environment_now, stdout=subprocess.PIPE, text=True, start_new_session=not systemd
        )
        if pinning:
            os.sched_setaffinity(0, allowed)
        children.append((unit, child))
        threads.append(threading.Thread(target=relay, args=(unit, child, reports), daemon=True))
        threads[-1].start()
        if systemd and kind == "server":
            hand_memory_peak(unit, child)

    escalations: list[threading.Timer] = []

    def stop(*_) -> None:
        if systemd:
            # Signalling `sudo` would reach `systemd-run` alone; stopping the units ends every process inside them.
            units = [f"{unit}.service" for unit, _ in children]
            subprocess.run(["sudo", "-n", "systemctl", "stop", *units], check=False)
            return
        signal_groups(signal.SIGINT)
        escalations.append(threading.Timer(STOP_SECONDS, signal_groups, args=(signal.SIGKILL,)))
        escalations[-1].daemon = True
        escalations[-1].start()

    def signal_groups(number: int) -> None:
        for _, child in children:
            try:
                os.killpg(child.pid, number)
            except ProcessLookupError:
                pass  # the group already ended

    signal.signal(signal.SIGTERM, stop)
    try:
        if server:
            addresses = [(arguments.parse_interface(p["environment"]["SERVEVAL_INTERFACES"].split(",")[0]).addresses[0],
                          int(p["environment"]["SERVEVAL_PORT"])) for p in placed]  # fmt: skip
            unready = wait_ready(addresses, [child for _, child in children])
            if unready is not None:
                stop()
                arguments.exit(f"{unready[0]} port {unready[1]} never accepted a connection")
            print(f"- Ready: {len(placed)} processes", file=sys.stderr, flush=True)
        if options.smoke:
            smoke = subprocess.run(
                shlex.split(options.smoke),
                env={**os.environ, "SERVEVAL_PORT": str(port), "SERVEVAL_WORKLOADS": "tool", "SERVEVAL_VARIANTS": "plain",
                     "SERVEVAL_PHASES": "steady", "SERVEVAL_CONNECTIONS": "64", "SERVEVAL_WARMUP": "100ms",
                     "SERVEVAL_TIME_LIMIT": "200ms", "SERVEVAL_BETWEEN_RUNS": "keep"},
                stdout=subprocess.PIPE, text=True, check=False,
            )  # fmt: skip
            measured = [json.loads(line) for line in smoke.stdout.splitlines() if line.startswith("{")]
            passed = (
                smoke.returncode == 0
                and measured
                and all(report["calls"] and not report["errors"] for report in measured)
            )
            stop()
            sys.exit(0 if passed else 1)
        codes = [child.wait() for _, child in children]
    except KeyboardInterrupt:
        stop()
        codes = [child.wait() for _, child in children]
    for escalation in escalations:
        escalation.cancel()  # every child was reaped
    if not systemd:
        signal_groups(signal.SIGKILL)  # workers a plain process left behind, which would hold its port and output
    for thread in threads:
        thread.join()
    if "SERVEVAL_BARRIER" in environment:
        shutil.rmtree(environment["SERVEVAL_BARRIER"], ignore_errors=True)
    if len(placed) > 1 and reports:
        merge_reports(reports, placed, domains, environment.get("SERVEVAL_OUTPUT", "results"))
    sys.exit(1 if any(code > 0 for code in codes) else 0)


if __name__ == "__main__":
    main()
