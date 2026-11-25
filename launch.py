#!/usr/bin/env python3
"""Starts a ServEval server or client as one process, one per interface on its memory domain, or several copies,
each on an even share of this launcher's cores and memory, and merges the reports of several client processes,
which split every connection count between them.

Programs named like `serv-eval-axum-server` or `fastapi_server.py` are servers, others clients. As root with systemd, every process is a transient unit with
`AllowedCPUs`, `MemoryMax` and a NUMA binding; otherwise plain processes pinned by affinity alone.
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


def unit_command(name: str, placement: dict, memory: int | None, environment: dict, command: list[str]) -> list[str]:
    """`command` as a transient systemd unit with the placement's cores, memory and NUMA binding."""
    files = int(pathlib.Path("/proc/sys/fs/nr_open").read_text())
    properties = [f"AllowedCPUs={spell_cpus(placement['cpus'])}", f"LimitNOFILE={files}"]
    if memory:
        properties.append(f"MemoryMax={memory}")
    if placement["domain"] >= 0:
        properties += ["NUMAPolicy=bind", f"NUMAMask={placement['domain']}", "CPUAffinity=numa"]
    setenv = [f"--setenv={key}={value}" for key, value in environment.items()]
    unit = ["systemd-run", "--wait", "--pipe", "--collect", "--same-dir", f"--unit={name}"]
    return [*unit, *(f"-p{property}" for property in properties), *setenv, *command]


def wait_ready(addresses: list[tuple[str, int]], children: list[subprocess.Popen], seconds: float = 300) -> None:
    """Returns once every address accepts a TCP connection; exits if a child dies or time runs out."""
    deadline = time.monotonic() + seconds
    for address in addresses:
        while True:
            try:
                socket.create_connection(address, timeout=1).close()
                break
            except OSError:
                if any(child.poll() is not None for child in children) or time.monotonic() > deadline:
                    arguments.exit(f"{address[0]} port {address[1]} never accepted a connection")
                time.sleep(0.05)


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.partition("\n")[0])
    parser.add_argument("--layout", choices=("one", "per-domain"), default="one", help="Processes per interface set.")
    parser.add_argument("--copies", type=arguments.parse_count, default=1, help="Copies of a server on adjacent ports.")
    parser.add_argument("--port", type=arguments.parse_port, default=None, help="First port, SERVEVAL_PORT by default.")
    parser.add_argument("--smoke", metavar="CLIENT", help="Run CLIENT for a 200ms steady tool exchange, then stop.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The binary and its flags, after --.")
    options = parser.parse_args()
    command = options.command[1:] if options.command[:1] == ["--"] else options.command
    if not command:
        arguments.exit("Name the binary after --, like ./launch.py -- target/release/serv-eval-axum-server")
    environment = {key: value for key, value in os.environ.items() if key.startswith("SERVEVAL_") or key in FORWARDED}
    interfaces = arguments.list_of(arguments.parse_interface)(os.environ.get("SERVEVAL_INTERFACES", "127.0.0.1"))
    targets = os.environ.get("SERVEVAL_TARGETS", "127.0.0.1").split(",")
    port = options.port or int(os.environ.get("SERVEVAL_PORT", "8545"))
    program = pathlib.Path(next((part for part in command if part.endswith(".py")), command[0])).stem
    role = program.removeprefix("serv-eval-").replace("-", "_")
    server = "_server" in role
    name = role.partition("_")[0]
    domains = domains_of(interfaces)
    placed = placements(options.layout, options.copies, interfaces, [] if server else targets, domains, port)
    budget = stats.unit_stats()
    memory = budget["memory_limit"] // len(placed) if budget["memory_limit"] else None
    spelled = ", ".join(f"{interface} (domain {domain})" for interface, domain in zip(interfaces, domains))
    print(f"- Interfaces: {spelled} · Layout: {len(placed)} processes", file=sys.stderr)
    for limit, value in results.kernel_limits().items():
        if value is not None:
            print(f"- {limit}: {value}", file=sys.stderr)
    if not server:
        wait_ready([(target, port) for target in targets], [])
        environment["SERVEVAL_BARRIER"] = tempfile.mkdtemp(prefix="serveval-barrier-")
    systemd = os.geteuid() == 0 and shutil.which("systemd-run") is not None
    children, threads, reports = [], [], []
    for index, placement in enumerate(placed):
        unit = f"serveval-{name}-{index}"
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
        child = subprocess.Popen(line, env=environment_now, stdout=subprocess.PIPE, text=True)
        if pinning:
            os.sched_setaffinity(0, allowed)
        children.append((unit, child))
        threads.append(threading.Thread(target=relay, args=(unit, child, reports), daemon=True))
        threads[-1].start()

    def stop(*_) -> None:
        for unit, child in children:
            if systemd:
                subprocess.run(["systemctl", "stop", f"{unit}.service"], check=False)
            child.send_signal(signal.SIGINT)

    signal.signal(signal.SIGTERM, stop)
    try:
        if server:
            addresses = [(arguments.parse_interface(p["environment"]["SERVEVAL_INTERFACES"].split(",")[0]).addresses[0],
                          int(p["environment"]["SERVEVAL_PORT"])) for p in placed]  # fmt: skip
            wait_ready(addresses, [child for _, child in children])
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
    for thread in threads:
        thread.join()
    if "SERVEVAL_BARRIER" in environment:
        shutil.rmtree(environment["SERVEVAL_BARRIER"], ignore_errors=True)
    if len(placed) > 1 and reports:
        merge_reports(reports, placed, domains, environment.get("SERVEVAL_OUTPUT", "results"))
    sys.exit(1 if any(code > 0 for code in codes) else 0)


if __name__ == "__main__":
    main()
