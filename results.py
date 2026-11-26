"""One JSON report per configuration: printed to stdout, echoed as a Markdown row to stderr, and written atomically as
`<framework>-<config-hash>.json`, with the same fields `results.rs` writes."""

import json
import os
import pathlib
import platform
import subprocess
import sys
import time

import arguments
import stats

# The report layout's version, which `scripts/plot.py` checks.
SCHEMA_VERSION = 3
HEADER = (
    "| Machine | Framework | Test | Workload | Parsing | Security | Phase | Connections | Calls per connection "
    "| calls/s | calls/s per server core | p50, µs | p99, µs | Body GB/s | Server CPU, µs/call | Client CPU, µs/call "
    "| KB per connection |\n"
    "| :------ | :-------- | :--- | :------- | :------ | :------- | :---- | ----------: | -------------------: "
    "| ------: | ----------------------: | ------: | ------: | --------: | ------------------: | ------------------: "
    "| ----------------: |"
)
# The raw counters that several client processes measuring one configuration together add up.
COUNTERS = (
    "calls",
    "errors",
    "unsent",
    "all_calls",
    "request_body_bytes",
    "response_body_bytes",
    "client_cpu_seconds",
    "client_cpu_percent",
)
# Counters of a whole box, which every process on it reads alike, so a merge keeps the largest.
BOX_COUNTERS = ("reconnects", "upstream_connections")
# Counters of the servers' units, which a merge adds once per distinct set of units.
SERVER_COUNTERS = (
    "server_cores",
    "server_memory_limit",
    "server_cpu_microseconds",
    "server_memory_peak_bytes",
    "server_memory_idle_peak_bytes",
)
# The sysctls that cap connection counts and socket memory, recorded with every report, as `launch.py` prints them.
KERNEL_LIMITS = (
    "fs.nr_open",
    "net.core.somaxconn",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.tcp_max_syn_backlog",
    "net.ipv4.tcp_max_tw_buckets",
    "net.ipv4.tcp_rmem",
    "net.ipv4.tcp_tw_reuse",
    "net.ipv4.tcp_wmem",
    "net.netfilter.nf_conntrack_max",
)
# Arm cores by `CPU implementer` and `CPU part`, named as `sysinfo` names them for `results.rs`.
ARM_PARTS = {
    (0x41, 0xD0C): "Neoverse-N1",
    (0x41, 0xD40): "Neoverse-V1",
    (0x41, 0xD49): "Neoverse-N2",
    (0x41, 0xD4F): "Neoverse-V2",
    (0x41, 0xD84): "Neoverse-V3",
    (0x41, 0xD8E): "Neoverse-N3",
}


def sysctl(name: str) -> str | None:
    try:
        output = subprocess.run(["sysctl", "-n", name], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return None
    return output or None


def cpu_model() -> str:
    """The first core's name: its Arm part where `ARM_PARTS` knows it, else its `model name`, as `sysinfo` reads it."""
    try:
        first = pathlib.Path("/proc/cpuinfo").read_text().partition("\n\n")[0]
    except OSError:
        return sysctl("machdep.cpu.brand_string") or ""
    fields = {key.strip(): value.strip() for key, _, value in (line.partition(":") for line in first.splitlines())}
    try:
        part = ARM_PARTS.get((int(fields["CPU implementer"], 16), int(fields["CPU part"], 16)))
    except (KeyError, ValueError):
        part = None
    return part or fields.get("model name", "")


def kernel_limits() -> dict[str, str | None]:
    """`KERNEL_LIMITS` as `/proc/sys` holds them, None where it does not."""
    limits = {}
    for name in KERNEL_LIMITS:
        try:
            limits[name] = " ".join((pathlib.Path("/proc/sys") / name.replace(".", "/")).read_text().split())
        except OSError:
            limits[name] = None
    return limits


def physical_cores() -> int | None:
    siblings = pathlib.Path("/sys/devices/system/cpu").glob("cpu[0-9]*/topology/core_cpus_list")
    cores = len({path.read_text() for path in siblings})
    if cores:
        return cores
    counted = sysctl("hw.physicalcpu")
    return int(counted) if counted else None


def os_version() -> str | None:
    if sys.platform == "darwin":
        return f"macOS {platform.mac_ver()[0]}"
    try:
        release = pathlib.Path("/etc/os-release").read_text()
    except OSError:
        return None
    names = dict(line.partition("=")[::2] for line in release.splitlines())
    return names.get("PRETTY_NAME", "").strip('"') or None


def ram_bytes() -> int:
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")


def budget(memory_limit: int | None) -> dict:
    """The environment's budget, narrowed by `--memory-limit`, which may not exceed the cgroup's limit."""
    cores = arguments.all_cores()
    cpus = f"0-{cores - 1}"
    try:
        for line in pathlib.Path("/proc/self/status").read_text().splitlines():
            if line.startswith("Cpus_allowed_list:"):
                cpus = line.partition(":")[2].strip()
    except OSError:
        pass
    environment = stats.unit_stats()["memory_limit"] or ram_bytes()
    if memory_limit and memory_limit > environment:
        arguments.exit(f"--memory-limit {memory_limit} exceeds the {environment} bytes this process may use")
    return {"cores": cores, "cpus": cpus, "memory_limit": memory_limit or environment}


def machine(budget: dict) -> dict:
    """The host's CPU, memory and OS, with this client's `budget`."""
    return {
        "cpu_model": cpu_model(),
        "logical_cores": os.cpu_count(),
        "physical_cores": physical_cores(),
        "ram_bytes": ram_bytes(),
        "kernel": platform.release() or None,
        "os": os_version(),
        "architecture": {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine()),
        "kernel_limits": kernel_limits(),
        "budget": budget,
    }


def server_cores_per_network_interface() -> int | None:
    """The server budget `SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE` names, as the launcher sets it, or None."""
    text = os.environ.get("SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE", "")
    if not text:
        return None
    if not (text.isascii() and text.isdigit()) or int(text) == 0:
        arguments.exit(
            f'SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE="{text}" does not parse, expected a positive count'
        )
    return int(text)


def config_hash(config: dict) -> str:
    """The FNV-1a hash of `config`'s compact JSON, as 16 hexadecimal digits, as `results.rs` computes it."""
    hash = 0xCBF29CE484222325
    for byte in json.dumps(config, separators=(",", ":"), ensure_ascii=False).encode():
        hash = ((hash ^ byte) * 0x100000001B3) & ((1 << 64) - 1)
    return f"{hash:016x}"


def report_path(directory: str, report: dict) -> pathlib.Path:
    return pathlib.Path(directory) / f"{report['config']['framework']}-{config_hash(report['config'])}.json"


def write_report(directory: str, report: dict) -> pathlib.Path:
    """Writes `report` at its `report_path`, flushed to the drive before it replaces an older one."""
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
    path = report_path(directory, report)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    with open(temporary, "w") as file:
        json.dump(report, file, indent=2)
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(path)
    return path


def derive(report: dict) -> dict:
    """Fills the rates, percentiles, per-call costs and the client-limited verdict from the raw counters, for one
    process or a merge: a row is client-limited when under 99% of the offered rate went out, or when its p99 send
    lateness exceeds 5% of its p50 latency, or when the client used over 90% of its own cores. A row the client could
    not run keeps its zeros."""
    if report["unsupported"]:
        return report
    seconds, offered = report["seconds"], report["config"]["offered_calls_per_second"]
    cpu, cores, calls = report["server_cpu_microseconds"], report["server_cores"], max(report["all_calls"], 1)
    peak, idle = report["server_memory_peak_bytes"], report["server_memory_idle_peak_bytes"]
    report["calls_per_second"] = report["calls"] / seconds
    report["calls_per_second_per_server_core"] = report["calls_per_second"] / cores if cores else None
    report["body_gigabytes_per_second"] = (report["request_body_bytes"] + report["response_body_bytes"]) / seconds / 1e9
    fractions = {"p50": 0.5, "p90": 0.9, "p99": 0.99, "p999": 0.999, "max": 1.0}
    for name in ("latency", "send_lateness"):
        bins = stats.dense(report[f"{name}_buckets"])
        report[f"{name}_us"] = {key: stats.percentile(bins, fraction) / 1000 for key, fraction in fractions.items()}
    # Both sides' CPU covers the warmup as well as the window, so both divide by every call.
    report["server_cpu_microseconds_per_call"] = None if cpu is None else cpu / calls
    report["client_cpu_microseconds_per_call"] = report["client_cpu_seconds"] * 1e6 / calls
    # The share of the serving cores the window's calls, failed ones included, kept busy at the server's CPU per call.
    per_call, rate = report["server_cpu_microseconds_per_call"], (report["calls"] + report["errors"]) / seconds
    report["server_busy_share"] = None if per_call is None or not cores else per_call * rate / 1e6 / cores
    connections = report["config"]["connections"]
    growth = None if None in (peak, idle) else max(peak - idle, 0)
    report["server_kilobytes_per_connection"] = None if growth is None else growth / 1024 / connections
    report["client_cpu_share"] = report["client_cpu_percent"] / 100 / report["machine"]["budget"]["cores"]
    offered_short = offered is not None and (report["calls"] + report["errors"]) / seconds < 0.99 * offered
    late = report["send_lateness_us"]["p99"] > 0.05 * report["latency_us"]["p50"]
    report["client_limited"] = offered_short or late or report["client_cpu_share"] > 0.9
    report["server_event_loop_counters_per_request"] = stats.per_request(report["server_event_loop_counters"])
    return report


def merge(reports: list[dict], domains: list[int]) -> dict:
    """One report from those of several client processes measuring the same configuration together, counting every
    interface and memory domain those processes used. Processes that sampled the same server units count them once,
    and rows the client could not run merge into one such row."""
    config = dict(reports[0]["config"], process=0, interfaces=len(domains))
    for field in ("threads", "connections", "in_flight", "offered_calls_per_second"):
        values = [report["config"][field] for report in reports]
        config[field] = None if None in values else sum(values)
    budgets = [report["machine"]["budget"] for report in reports]
    budget = {
        "cores": sum(budget["cores"] for budget in budgets),
        "cpus": ",".join(budget["cpus"] for budget in budgets),
        "memory_limit": sum(budget["memory_limit"] for budget in budgets),
    }
    merged = dict(reports[0], config=config, machine=dict(reports[0]["machine"], budget=budget))
    merged["domains"] = len(set(domains))
    windows = [report["window_unix_ms"] for report in reports]
    merged["window_unix_ms"] = [min(window[0] for window in windows), max(window[1] for window in windows)]
    for field in COUNTERS:
        merged[field] = sum(report[field] for report in reports)
    merged["error_classes"] = {
        name: sum(report["error_classes"][name] for report in reports) for name in stats.FAILURES
    }
    sampled, seen = [], set()
    for report in reports:
        units = tuple(report["server_units"])
        if units and units in seen:
            continue
        seen.add(units)
        sampled.append(report)
    merged["server_units"] = list(dict.fromkeys(unit for report in sampled for unit in report["server_units"]))
    cpus = (cpu for report in sampled for cpu in stats.parse_cpus(report["server_cpu_list"] or ""))
    merged["server_cpu_list"] = stats.spell_cpus(cpus) or None
    for field in SERVER_COUNTERS:
        values = [report[field] for report in sampled]
        merged[field] = None if None in values else sum(values)
    merged["server_event_loop_counters"] = stats.summed_counters(
        [report["server_event_loop_counters"] for report in sampled]
    )
    for field in BOX_COUNTERS:
        values = [report[field] for report in reports]
        merged[field] = None if None in values else max(values)
    for field in ("latency_buckets", "send_lateness_buckets"):
        bins = [0] * stats.BINS
        for report in reports:
            bins = [mine + theirs for mine, theirs in zip(bins, stats.dense(report[field]))]
        merged[field] = stats.sparse(bins)
    return derive(merged)


class Report:
    """Prints the settings, then one Markdown and one JSON line per configuration, and writes the files."""

    def __init__(self, args, framework: str, client: str, batch_size: int | None = None) -> None:
        """`client` names this client program; `batch_size` is the server's from `/health`, None when it does not
        batch."""
        self.args, self.framework, self.client, self.batch_size = args, framework, client, batch_size
        self.process = arguments.process()
        self.started_unix_seconds = int(time.time())
        self.printed_header = False
        self.server_cores_per_network_interface = server_cores_per_network_interface()
        resolved = budget(args.memory_limit)

        def joined(values) -> str:
            return ",".join(map(str, values))

        def optional(count: int | None) -> str:
            return "none" if count is None else str(count)

        settings = (
            ("Framework", framework), ("Client", client), ("Threads", args.threads),
            ("Interfaces", joined(args.interfaces)), ("Port", args.port), ("Security", args.security),
            ("Data dir", args.data_dir), ("Seed", args.seed), ("Targets", joined(args.targets)),
            ("Connections", joined(args.connections)), ("Workloads", joined(args.workloads)),
            ("Variants", joined(args.variants)), ("Phases", joined(args.phases)), ("Parsing", joined(args.parsing)),
            ("Body bytes", joined(map(arguments.spell_size, arguments.body_bytes(args)))),
            ("Calls in flight per connection", joined(args.calls_in_flight_per_connection)),
            ("Calls per second", optional(args.calls_per_second)), ("Width", args.width),
            ("Time limit", arguments.spell_duration(args.time_limit)), ("Warmup", arguments.spell_duration(args.warmup)),
            ("Between runs", args.between_runs), ("Output", args.output),
            ("Cores", f"{resolved['cores']} ({resolved['cpus']})"),
            ("Memory limit", arguments.spell_size(resolved["memory_limit"])),
        )  # fmt: skip
        for name, value in settings:
            print(f"- {name}: {value}", file=sys.stderr)
        self.machine = machine(resolved)

    def config(self, run: arguments.Run) -> dict:
        """The fields that name `run`'s configuration, which the file name hashes; launch.py merges reports equal in
        all but `process`, `threads`, `connections` and `in_flight`."""
        args = self.args
        config = {
            "framework": self.framework,
            "client": self.client,
            "language": "python",
            "test": run.test,
            "workload": run.workload,
            "variant": run.variant,
            "parsing": run.parsing,
            "security": "tls13" if args.security == "tls" else args.security,
            "phase": run.phase,
            "process": self.process[0],
            "processes": self.process[1],
            "interfaces": len(args.interfaces),
            "threads": args.threads,
            "connections": run.connections,
            "calls_in_flight_per_connection": run.calls_in_flight_per_connection,
            "calls_in_flight_per_connection_source": run.calls_in_flight_per_connection_source,
            "in_flight": run.in_flight,
            "pipeline_depth": 1,
            "body_bytes": run.body_bytes,
            "width": args.width,
            "offered_calls_per_second": args.calls_per_second,
            "server_cores_per_network_interface": self.server_cores_per_network_interface,
            "seed": args.seed,
            "between_runs": args.between_runs,
        }
        # Only batched runs of a batching server have a batch size; the others leave the field out of their hash.
        if run.variant == "batched" and self.batch_size is not None:
            config["batch_size"] = self.batch_size
        return config

    def unsupported(self, run: arguments.Run, reason: str) -> pathlib.Path:
        """Writes the row of a configuration that cannot run, `unsupported` naming why: its configuration and machine,
        with zeros where a run would have measured, since no call went out and no server was sampled."""
        latency = dict.fromkeys(("p50", "p90", "p99", "p999", "max"), 0.0)
        report = {
            "schema_version": SCHEMA_VERSION,
            "started_unix_seconds": self.started_unix_seconds,
            "machine": self.machine,
            "config": self.config(run),
            "unsupported": reason,
            "ucall_commit": None,
            "tls_suite": None,
            "domains": None,
            "seconds": 0.0,
            "window_unix_ms": [0, 0],
            "calls": 0,
            "errors": 0,
            "error_classes": dict.fromkeys(stats.FAILURES, 0),
            "unsent": 0,
            "all_calls": 0,
            "request_body_bytes": 0,
            "response_body_bytes": 0,
            "reconnects": None,
            "upstream_connections": None,
            "server_units": [],
            "server_cpu_list": None,
            "server_cores": None,
            "server_memory_limit": None,
            "server_cpu_microseconds": None,
            "server_memory_peak_bytes": None,
            "server_memory_idle_peak_bytes": None,
            "client_cpu_seconds": 0.0,
            "calls_per_second": 0.0,
            "calls_per_second_per_server_core": None,
            "body_gigabytes_per_second": 0.0,
            "latency_us": latency,
            "latency_buckets": [],
            "send_lateness_us": dict(latency),
            "send_lateness_buckets": [],
            "server_cpu_microseconds_per_call": None,
            "client_cpu_microseconds_per_call": None,
            "server_busy_share": None,
            "server_kilobytes_per_connection": None,
            "server_event_loop_counters": None,
            "server_event_loop_counters_per_request": None,
            "client_cpu_percent": 0.0,
            "client_cpu_share": 0.0,
            "client_limited": False,
        }
        self.print(report)
        return write_report(self.args.output, report)

    def add(self, run: arguments.Run, window: stats.Window, tally: stats.Tally, sample: dict) -> pathlib.Path:
        """Assembles the report of `run` from the counters around it: `sample` holds `server_before` and
        `server_after`, as `stats.server_stats` sums them, `client_cpu` seconds and `client_opens`, the client box's
        `tcp_active_opens` as the window opened and closed, plus `tls_suite` when one was negotiated. The server's
        memory peak before connecting covers the idle time since the previous sample, and after the window, the
        connections and the window."""
        args, before, after = self.args, sample["server_before"], sample["server_after"]

        def opens(before: int | None, after: int | None) -> int | None:
            return None if None in (before, after) else max(after - before, 0)

        cpu = (
            None
            if None in (after["cpu_microseconds"], before["cpu_microseconds"])
            else max(after["cpu_microseconds"] - before["cpu_microseconds"], 0)
        )
        report = {
            "schema_version": SCHEMA_VERSION,
            "started_unix_seconds": self.started_unix_seconds,
            "machine": self.machine,
            "config": self.config(run),
            "unsupported": None,
            "ucall_commit": None,
            "tls_suite": sample.get("tls_suite"),
            "domains": None,
            "seconds": window.seconds,
            "window_unix_ms": window.unix_ms(),
            "calls": tally.calls,
            "errors": sum(tally.failures.values()),
            "error_classes": tally.failures,
            "unsent": tally.unsent,
            "all_calls": tally.all_calls,
            "request_body_bytes": tally.request_body_bytes,
            "response_body_bytes": tally.response_body_bytes,
            "reconnects": opens(*sample["client_opens"]),
            "upstream_connections": opens(before["tcp_active_opens"], after["tcp_active_opens"]),
            "server_units": after["units"],
            "server_cpu_list": after["cpu_list"] or None,
            "server_cores": after["cores"] or None,
            "server_memory_limit": after["memory_limit"],
            "server_cpu_microseconds": cpu,
            "server_memory_peak_bytes": after["memory_peak_bytes"],
            "server_memory_idle_peak_bytes": before["memory_peak_bytes"],
            "client_cpu_seconds": sample["client_cpu"],
            "calls_per_second": None,
            "calls_per_second_per_server_core": None,
            "body_gigabytes_per_second": None,
            "latency_us": None,
            "latency_buckets": stats.sparse(tally.bins),
            "send_lateness_us": None,
            "send_lateness_buckets": stats.sparse(tally.lateness),
            "server_cpu_microseconds_per_call": None,
            "client_cpu_microseconds_per_call": None,
            "server_busy_share": None,
            "server_kilobytes_per_connection": None,
            "server_event_loop_counters": stats.counter_growth(
                before.get("event_loop_counters"), after.get("event_loop_counters")
            ),
            "server_event_loop_counters_per_request": None,
            "client_cpu_percent": 100 * sample["client_cpu"] / (args.warmup + args.time_limit),
            "client_cpu_share": None,
            "client_limited": None,
        }
        derive(report)
        self.print(report)
        return write_report(args.output, report)

    def print(self, report: dict) -> None:
        if not self.printed_header:
            print(HEADER, file=sys.stderr)
            self.printed_header = True

        def optional(value, digits: int = 1):
            return "n/a" if value is None else f"{value:.{digits}f}"

        config = report["config"]
        cells = (
            self.machine["cpu_model"], config["framework"], config["test"], config["workload"], config["parsing"],
            config["security"], config["phase"], config["connections"], config["calls_in_flight_per_connection"],
        )  # fmt: skip
        if report["unsupported"]:
            cells += (f"unsupported: {report['unsupported']}",)
        else:
            cells += (
                f"{report['calls_per_second']:.0f}", optional(report["calls_per_second_per_server_core"], 0),
                f"{report['latency_us']['p50']:.1f}", f"{report['latency_us']['p99']:.1f}",
                f"{report['body_gigabytes_per_second']:.3f}", optional(report["server_cpu_microseconds_per_call"]),
                optional(report["client_cpu_microseconds_per_call"]), optional(report["server_kilobytes_per_connection"]),
            )  # fmt: skip
        print("| " + " | ".join(map(str, cells)) + " |", file=sys.stderr)
        print(json.dumps(report, separators=(",", ":")), flush=True)
