# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Print ServEval JSON reports as one Markdown table per test, or plot them as SVG with --output.

Reports come from files or directories, searched recursively, skipping `quarantine` directories.
With --records, every row gets each network interface's received and transmitted rates and allowance counter
differences, from the `scripts/record.sh` samples that bracket its window, with the server's cards' Gbit/s in each
direction at its top level, and a ceiling table names where each allowance was first exceeded. A row is void only
when no samples bracket its window. Where the server's records hold per-core CPU samples, every row also gets the
interrupt and softirq time of the server's cores outside its serving cores per call, the receive and transmit work
the kernel does for the server on the interrupt cores, which the server's own CPU per call leaves out.
"""

import argparse
import bisect
import json
import sys
from pathlib import Path

THEME = {
    "field": "#C44536",
    "ink": "#FFE6A7",
    "frame": "#E5E5E5",
    "font": ["Poppins", "Helvetica", "Arial", "sans-serif"],
}
TESTS = ("latency", "throughput")
WORKLOADS = ("tool", "context", "download", "fanout")
MINIMUM_SCHEMA_VERSION = 2
ALLOWANCES = (
    "bw_in_allowance_exceeded",
    "bw_out_allowance_exceeded",
    "pps_allowance_exceeded",
    "conntrack_allowance_exceeded",
    "linklocal_allowance_exceeded",
)
# Which direction's rate each allowance limits; packets, tracked connections and link-local traffic count both ways.
ALLOWANCE_DIRECTIONS = {"bw_in_allowance_exceeded": "received", "bw_out_allowance_exceeded": "transmitted"}
# ENA reports no device totals, only per-queue counters, named like `queue_3_rx_bytes`.
QUEUE_COUNTERS = {
    "rx_bytes": "received_bytes",
    "tx_bytes": "transmitted_bytes",
    "rx_cnt": "received_packets",
    "tx_cnt": "transmitted_packets",
}
MARKERS = ["o", "s", "D", "^", "v", "P", "X", "*"]


def parse_cpus(text: str) -> list[int]:
    cpus = []
    for part in filter(None, text.strip().split(",")):
        first, _, last = part.partition("-")
        cpus += range(int(first), int(last or first) + 1)
    return cpus


def server_cores(report: dict) -> int | None:
    """The cores every server process of the row had together, recorded or counted from its CPU list."""
    if report.get("server_cores") is not None:
        return report["server_cores"]
    listed = report.get("server_cpu_list")
    return len(parse_cpus(listed)) if listed else None


def calls_in_flight_per_connection(report: dict) -> float | None:
    """The recorded depth, or, for rows before it was recorded, the calls in flight over the connections."""
    config = report["config"]
    if config.get("calls_in_flight_per_connection") is not None:
        return config["calls_in_flight_per_connection"]
    if config.get("in_flight") is None:
        return None
    return config["in_flight"] / config["connections"]


def test_of(report: dict) -> str:
    """The recorded test, or for older rows the one their depth matches: one call per connection is the latency test."""
    if report["config"].get("test"):
        return report["config"]["test"]
    return "latency" if calls_in_flight_per_connection(report) in (None, 1) else "throughput"


def per_server_core(report: dict) -> float | None:
    if report.get("calls_per_second_per_server_core") is not None:
        return report["calls_per_second_per_server_core"]
    cores = server_cores(report)
    return report["calls_per_second"] / cores if cores and report.get("calls_per_second") is not None else None


def first_of(report: dict, *names: str):
    """The first of `names` the report recorded, so rows written before a rename still fill their column."""
    return next((report[name] for name in names if report.get(name) is not None), None)


def machine_column(report: dict, machine_name: str | None) -> str:
    """Like `AWS Graviton4, 192 cores, 4 threads, 2 domains`: the server box and the row's server budget."""
    machine = report["machine"]
    name = machine_name or machine.get("cpu_name") or machine.get("cpu_model") or "-"
    threads = server_cores(report)
    domains = first_of(report, "server_memory_domains", "server_domains")
    return f"{name}, {cell(machine.get('logical_cores'))} cores, {cell(threads)} threads, {cell(domains)} domains"


def server_cards(report: dict, direction: str) -> dict[str, float]:
    """Each server card's Gbit/s in `direction`, `received` or `transmitted`, over the row's window, empty when the
    server side was not recorded."""
    cards = report.get("network_interfaces", {}).get("server", {})
    return {card: figures[f"gigabits_per_second_{direction}"] for card, figures in sorted(cards.items())}


def card_names(report: dict) -> str | None:
    """The recorded server cards, in the order the per-card columns list their rates."""
    figures = report.get("server_gigabits_per_second_received_per_network_interface")
    return " / ".join(sorted(figures)) if figures else None


def per_card(report: dict, direction: str) -> str | None:
    """The server cards' Gbit/s in `direction`, in the order the Server cards setting names them."""
    figures = report.get(f"server_gigabits_per_second_{direction}_per_network_interface")
    return " / ".join(cell(value) for _, value in sorted(figures.items())) if figures else None


def exceeded_allowances(report: dict) -> str:
    """Every allowance counter that rose in the row's window, as `machine card counter`."""
    names = [
        f"{machine} {card} {name.removesuffix('_allowance_exceeded')}"
        for machine, cards in sorted(report.get("network_interfaces", {}).items())
        for card, figures in sorted(cards.items())
        for name, rise in figures["allowance_exceeded"].items()
        if rise > 0
    ]
    return ", ".join(names) if names else ("-" if not report.get("network_interfaces") else "none")


COLUMNS = (
    ("Framework", lambda report: report["config"]["framework"], ":--"),
    ("Client", lambda report: report["config"]["client"], ":--"),
    ("Language", lambda report: report["config"]["language"], ":--"),
    ("Workload", lambda report: report["config"]["workload"], ":--"),
    ("Connections", lambda report: report["config"]["connections"], "--:"),
    ("In flight per connection", calls_in_flight_per_connection, "--:"),
    ("calls/s", lambda report: report["calls_per_second"], "--:"),
    ("calls/s per server core", per_server_core, "--:"),
    ("p50, µs", lambda report: report["latency_us"]["p50"], "--:"),
    ("p99, µs", lambda report: report["latency_us"]["p99"], "--:"),
    ("p999, µs", lambda report: report["latency_us"]["p999"], "--:"),
    ("Server CPU, µs/call", lambda r: first_of(r, "server_cpu_microseconds_per_call", "server_cpu_us_per_call"), "--:"),
    ("Interrupt CPU, µs/call", lambda report: report.get("server_interrupt_cpu_microseconds_per_call"), "--:"),
    ("Server busy share", lambda report: report.get("server_busy_share"), "--:"),
    ("Client CPU, µs/call", lambda report: report.get("client_cpu_microseconds_per_call"), "--:"),
    ("KB per connection", lambda r: first_of(r, "server_kilobytes_per_connection", "server_kb_per_connection"), "--:"),
    ("Errors", lambda report: report["errors"], "--:"),
    ("Server Gbit/s in per card", lambda report: per_card(report, "received"), "--:"),
    ("Server Gbit/s out per card", lambda report: per_card(report, "transmitted"), "--:"),
    ("Allowances exceeded", exceeded_allowances, ":--"),
)
# Settings a test's table names once above itself when every row shares them, and as columns when they differ.
SETTINGS = (
    ("Server cores per network interface", lambda r: r["config"].get("server_cores_per_network_interface"), "--:"),
    ("Server CPU list", lambda report: report.get("server_cpu_list"), ":--"),
    ("Variant", lambda report: report["config"]["variant"], ":--"),
    ("Parsing", lambda report: report["config"]["parsing"], ":--"),
    ("Security", lambda report: report["config"]["security"], ":--"),
    ("Phase", lambda report: report["config"]["phase"], ":--"),
    ("Body", lambda report: report["config"]["body_bytes"], "--:"),
    ("Server cards", lambda report: card_names(report), ":--"),
)
CLIENT_COLUMNS = (
    ("Framework", lambda report: report["config"]["framework"], ":--"),
    ("Client", lambda report: report["config"]["client"], ":--"),
    ("Language", lambda report: report["config"]["language"], ":--"),
    ("Test", test_of, ":--"),
    ("Workload", lambda report: report["config"]["workload"], ":--"),
    ("Connections", lambda report: report["config"]["connections"], "--:"),
    ("calls/s", lambda report: report["calls_per_second"], "--:"),
    ("Client CPU, µs/call", lambda report: report.get("client_cpu_microseconds_per_call"), "--:"),
    ("Client CPU share", lambda report: report.get("client_cpu_share"), "--:"),
    ("Client-limited", lambda report: "yes" if report.get("client_limited") else "no", ":--"),
)
METRICS = (
    ("throughput", "Calls / second", lambda report: report["calls_per_second"]),
    ("latency", "p99 latency (µs)", lambda report: report["latency_us"]["p99"]),
)


def cell(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and value.is_integer() and abs(value) < 100:
        return str(int(value))
    if isinstance(value, float):
        size = abs(value)
        return f"{value:,.0f}" if size >= 100 else f"{value:.1f}" if size >= 1 else f"{value:.3f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def series(report: dict) -> str:
    config = report["config"]
    parts = [config["framework"], config["client"], config["language"], config["variant"], config["parsing"]]
    parts += [config["security"], config["phase"]]
    if config.get("server_cores_per_network_interface") is not None:
        parts.append(f"{config['server_cores_per_network_interface']} cores per interface")
    if config["workload"] in ("context", "download"):
        parts.append(f"{config['body_bytes']:,} B")
    return " · ".join(parts)


def read_card(path: Path) -> list[tuple[int, dict[str, int]]]:
    """One card's `ethtool -S` samples as Unix milliseconds and the summed queue and allowance counters, dropping
    samples the recorder left incomplete, like the last one before it was stopped."""
    samples, queues = [], []
    with open(path) as file:
        for line in file:
            if line.startswith("@"):
                samples.append((int(line[1:]), {}))
                queues.append(0)
                continue
            if not samples:
                continue
            name, _, value = line.strip().partition(": ")
            if name.startswith("queue_"):
                counter = QUEUE_COUNTERS.get(name.split("_", 2)[2])
                if counter:
                    counters = samples[-1][1]
                    counters[counter] = counters.get(counter, 0) + int(value)
                    queues[-1] += 1
            elif name in ALLOWANCES:
                samples[-1][1][name] = int(value)
    complete = max(queues, default=0)
    return [
        sample
        for sample, counted in zip(samples, queues)
        if counted == complete and all(name in sample[1] for name in ALLOWANCES)
    ]


def read_cpu(path: Path) -> list[tuple[int, dict[int, int]]]:
    """One machine's `/proc/stat` samples as Unix milliseconds and each core's interrupt and softirq time in
    microseconds, dropping samples with fewer cores than the most complete one."""
    samples, ticks_per_second = [], 100
    with open(path) as file:
        for line in file:
            if line.startswith("clock_ticks_per_second "):
                ticks_per_second = int(line.split()[1])
            elif line.startswith("@"):
                samples.append((int(line[1:]), {}))
            elif samples and line.startswith("cpu"):
                # cpuN user nice system idle iowait irq softirq ...
                fields = line.split()
                ticks = int(fields[6]) + int(fields[7])
                samples[-1][1][int(fields[0][3:])] = ticks * 1_000_000 // ticks_per_second
    complete = max((len(cores) for _, cores in samples), default=0)
    return [sample for sample in samples if len(sample[1]) == complete]


def interrupt_cpu_per_call(report: dict, runs: list[list[tuple[int, dict[int, int]]]]) -> float | None:
    """The interrupt and softirq microseconds of every server core outside the row's serving cores per call over
    the samples bracketing its window, or None when no run brackets it or the row names no serving cores."""
    window, serving = report.get("window_unix_ms"), set(parse_cpus(report.get("server_cpu_list") or ""))
    if not window or not serving or not report.get("calls_per_second"):
        return None
    start, end = window
    for samples in runs:
        stamps = [stamp for stamp, _ in samples]
        before, after = bisect.bisect_right(stamps, start) - 1, bisect.bisect_left(stamps, end)
        if before < 0 or after == len(samples):
            continue
        (first, earlier), (last, later) = samples[before], samples[after]
        spent = sum(later[core] - earlier.get(core, later[core]) for core in later if core not in serving)
        calls = report["calls_per_second"] * (last - first) / 1000
        return spent / calls if calls else None
    return None


def read_records(directories: list[Path]) -> dict[tuple[str, str], list[list[tuple[int, dict[str, int]]]]]:
    """Every card's sample runs per machine, from each directory's own files and from one subdirectory per run.
    The machine is the directory's name without `records-`, like `server` for `records-server`."""
    runs: dict[tuple[str, str], list] = {}
    for directory in directories:
        machine = directory.name.removeprefix("records-")
        for path in sorted([*directory.glob("ethtool-*.log"), *directory.glob("*/ethtool-*.log")]):
            samples = read_card(path)
            if samples:
                card = path.stem.removeprefix("ethtool-")
                runs.setdefault((machine, card), []).append(samples)
    return runs


def card_window(runs: list[list[tuple[int, dict[str, int]]]], window: list[int]) -> dict | None:
    """One card's rates and allowance rises over a window, from the last sample at or before its start and the first
    at or after its end in one run, or None when no run brackets it or a counter reset inside it."""
    start, end = window
    for samples in runs:
        stamps = [stamp for stamp, _ in samples]
        before, after = bisect.bisect_right(stamps, start) - 1, bisect.bisect_left(stamps, end)
        if before < 0 or after == len(samples):
            continue
        (first, earlier), (last, later) = samples[before], samples[after]
        rises = {name: later[name] - earlier[name] for name in (*QUEUE_COUNTERS.values(), *ALLOWANCES)}
        if any(rise < 0 for rise in rises.values()):
            return None
        # The bracket is up to one sampling period wider at each end than the window, so rates divide by it.
        seconds = (last - first) / 1000
        return {
            "sampled_seconds": seconds,
            "gigabits_per_second_received": rises["received_bytes"] * 8 / seconds / 1e9,
            "gigabits_per_second_transmitted": rises["transmitted_bytes"] * 8 / seconds / 1e9,
            "millions_of_packets_per_second_received": rises["received_packets"] / seconds / 1e6,
            "millions_of_packets_per_second_transmitted": rises["transmitted_packets"] / seconds / 1e6,
            "allowance_exceeded": {name: rises[name] for name in ALLOWANCES},
        }
    return None


def network_interfaces(report: dict, records: dict) -> dict:
    """Every recorded card's figures over the row's window, as `{machine: {card: figures}}`."""
    window = report.get("window_unix_ms")
    found: dict[str, dict] = {}
    if not window:
        return found
    for (machine, card), runs in sorted(records.items()):
        figures = card_window(runs, window)
        if figures is not None:
            found.setdefault(machine, {})[card] = figures
    return found


def read_reports(paths: list[Path]) -> list[tuple[Path, dict]]:
    """Every report under `paths`, once each, though links like a phase's `rows/` name it twice."""
    found, seen = [], set()
    for path in paths:
        candidates = sorted(path.rglob("*.json")) if path.is_dir() else [path]
        for candidate in candidates:
            if "quarantine" in candidate.parts or candidate.resolve() in seen:
                continue
            seen.add(candidate.resolve())
            report = json.loads(candidate.read_text())
            if not isinstance(report, dict) or "config" not in report:
                print(f"- Skipped {candidate}: not a ServEval report", file=sys.stderr)
                continue
            if report.get("schema_version", 0) < MINIMUM_SCHEMA_VERSION:
                raise ValueError(f"Unsupported report schema: {candidate}")
            found.append((candidate, report))
    return found


def print_table(reports: list[dict], columns, machine_name: str | None, settings=SETTINGS) -> None:
    """One Markdown table, naming above it each setting every row shares, and with the rest as columns."""
    shared, varying = [], []
    for setting in settings:
        values = {cell(setting[1](report)) for report in reports}
        (shared if len(values) == 1 else varying).append((setting, values.pop() if len(values) == 1 else None))
    named = [(title, value) for (title, _, _), value in shared if value != "-"]
    for title, value in named:
        print(f"- {title}: {value}")
    every = [("Machine", lambda report: machine_column(report, machine_name), ":--"), *columns]
    every += [setting for setting, _ in varying]
    print(("\n" if named else "") + "| " + " | ".join(title for title, _, _ in every) + " |")
    print("| " + " | ".join(alignment for _, _, alignment in every) + " |")
    for report in reports:
        print("| " + " | ".join(cell(value(report)) for _, value, _ in every) + " |")


def order(report: dict) -> tuple:
    config = report["config"]
    workload = WORKLOADS.index(config["workload"]) if config["workload"] in WORKLOADS else len(WORKLOADS)
    cores = server_cores(report) or 0
    return workload, config["connections"], cores, config["body_bytes"], -(report.get("calls_per_second") or 0)


def print_results(reports: list[dict], machine_name: str | None) -> None:
    tests = sorted({test_of(report) for report in reports}, key=lambda test: (test not in TESTS, test))
    for test in tests:
        selected = sorted((report for report in reports if test_of(report) == test), key=order)
        print(f"\n## {test.capitalize()} Test\n")
        print_table(selected, COLUMNS, machine_name)


def print_ceiling(reports: list[dict]) -> None:
    """Where each allowance was first exceeded, on each machine's card, with that row's rate in its direction."""
    first: dict[tuple[str, str, str], tuple[dict, dict]] = {}
    counts: dict[tuple[str, str, str], int] = {}
    for report in sorted(reports, key=lambda report: report["window_unix_ms"][0]):
        for machine, cards in report.get("network_interfaces", {}).items():
            for card, figures in cards.items():
                for name, rise in figures["allowance_exceeded"].items():
                    if rise > 0:
                        key = (machine, card, name)
                        first.setdefault(key, (report, figures))
                        counts[key] = counts.get(key, 0) + 1
    print("\n## Network Ceiling\n")
    if not first:
        print("No allowance counter rose in any recorded row.")
        return
    print("| Network interface | Direction | Allowance exceeded | At | Rows exceeding | First row that exceeded it |")
    print("| :---------------- | :-------- | :----------------- | -: | -------------: | :------------------------- |")
    for (machine, card, name), (report, figures) in sorted(first.items()):
        direction = ALLOWANCE_DIRECTIONS.get(name, "both")
        directions = ("received", "transmitted") if direction == "both" else (direction,)
        gigabits = sum(figures[f"gigabits_per_second_{way}"] for way in directions)
        packets = sum(figures[f"millions_of_packets_per_second_{way}"] for way in directions)
        config = report["config"]
        cores = config.get("server_cores_per_network_interface")
        described = [config["framework"], test_of(report), config["workload"], f"{config['connections']:,}"]
        described.append(f"{cores} per interface" if cores is not None else f"{cell(server_cores(report))} cores")
        rate = f"{gigabits:,.1f} Gbit/s, {packets:,.2f} Mpps"
        print(f"| {machine} {card} | {direction} | {name} | {rate} | {counts[(machine, card, name)]} | "
              + ", ".join(described) + " |")  # fmt: skip


def print_unsupported(reports: list[dict]) -> None:
    print("\n## Unsupported\n")
    print("| Framework | Client | Test | Workload | Connections | In flight per connection | Reason |")
    print("| :-------- | :----- | :--- | :------- | ----------: | -----------------------: | :----- |")
    for report in reports:
        config = report["config"]
        cells = [config["framework"], config["client"], test_of(report), config["workload"]]
        cells += [cell(config.get("connections")), cell(calls_in_flight_per_connection(report))]
        print("| " + " | ".join(cells) + f" | {report['unsupported']} |")


def print_voided(voided: list[tuple[dict, str]]) -> None:
    print("\n## Void\n")
    print("| Framework | Test | Workload | Connections | Window, Unix ms | Reason |")
    print("| :-------- | :--- | :------- | ----------: | :-------------- | :----- |")
    for report, reason in voided:
        config = report["config"]
        window = "-".join(map(str, report.get("window_unix_ms") or []))
        cells = (config["framework"], test_of(report), config["workload"], cell(config["connections"]), window)
        print("| " + " | ".join(cells) + f" | {reason} |")


def plot(reports: list[dict], output: Path) -> None:
    plt = themed_pyplot()
    output.mkdir(parents=True, exist_ok=True)
    for test in sorted({test_of(report) for report in reports}):
        for workload in WORKLOADS:
            selected = [r for r in reports if r["config"]["workload"] == workload and test_of(r) == test]
            if not selected:
                continue
            lines: dict[str, list[dict]] = {}
            for report in sorted(selected, key=lambda report: report["config"]["connections"]):
                lines.setdefault(series(report), []).append(report)
            for name, label, value in METRICS:
                figure, axis = plt.subplots(figsize=(16, 9.2))
                for index, (title, points) in enumerate(lines.items()):
                    axis.plot(
                        [report["config"]["connections"] for report in points],
                        [value(report) for report in points],
                        marker=MARKERS[index % len(MARKERS)],
                        color=THEME["ink"],
                        alpha=0.5 + 0.5 * (points[0]["config"]["framework"] == "ucall"),
                        label=title,
                    )
                axis.set_xscale("log")
                axis.set_yscale("log")
                title = f"{workload.capitalize()} calls, {test} test\n{selected[0]['machine']['cpu_model']}"
                axis.set_title(title, loc="left")
                axis.set_xlabel("Connections")
                axis.set_ylabel(label)
                axis.grid(alpha=0.2)
                axis.legend(loc="best", frameon=False)
                figure.savefig(output / f"{test}-{workload}-{name}.svg", format="svg", bbox_inches="tight")
                plt.close(figure)
    print(output)


# Article figures, drawn with --figures. Each figure is one or more SVGs, each with a Markdown sidecar naming the
# rows it draws, the configuration they share, the machine, and every matching row it leaves out and why.
FIGURES = ("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f9", "f10")
# The server's direction each body-carrying workload moves its bytes in: uploads arrive, downloads leave.
WORKLOAD_DIRECTIONS = {"context": "received", "download": "transmitted"}
SECURITY_MODES = ("none", "noise-ik", "noise-xx", "tls")
PERCENTILES = ("p50", "p99", "p999")
# Config fields that may split one figure's rows into more series, named in the series label only when they vary.
SERIES_FIELDS = ("variant", "parsing", "security", "phase", "calls_in_flight_per_connection")
HATCHES = ["", "//", "..", "xx", "\\\\", "oo", "++", "--"]
LINE_STYLES = ["-", "--", ":", "-."]


def themed_pyplot():
    """pyplot with every chart's colors and fonts from THEME, writing text as SVG text, which the reader's browser
    sets in the first of THEME's fonts it has, so a missing font here is no warning."""
    import logging

    import matplotlib

    logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "figure.facecolor": THEME["field"],
            "axes.facecolor": THEME["field"],
            "axes.edgecolor": THEME["frame"],
            "axes.labelcolor": THEME["ink"],
            "axes.titlecolor": THEME["ink"],
            "text.color": THEME["ink"],
            "xtick.color": THEME["ink"],
            "ytick.color": THEME["ink"],
            "grid.color": THEME["ink"],
            "font.family": THEME["font"],
            "svg.fonttype": "none",
            "hatch.color": THEME["field"],
        }
    )
    return plt


def cores_per_interface(report: dict) -> int | None:
    return report["config"].get("server_cores_per_network_interface")


def spell_cores(count: int | None) -> str:
    return f"{cell(count)} {'core' if count == 1 else 'cores'} per interface"


def stack(report: dict, clients_vary: bool = False) -> str:
    """The serving stack, like `axum` or `ucall rust`, naming its client too when one figure pairs a server with
    several clients."""
    config = report["config"]
    name = config["framework"] if config["framework"] != "ucall" else f"ucall {config['language']}"
    return f"{name} server, {config['client']} client" if clients_vary else name


def series_of(rows: list[dict]):
    """A labeller of `rows` by stack, adding each of SERIES_FIELDS that differs between them."""
    varying = [field for field in SERIES_FIELDS if len({row["config"].get(field) for row in rows}) > 1]
    frameworks = {}
    for row in rows:
        frameworks.setdefault(row["config"]["framework"], set()).add(row["config"]["client"])
    clients_vary = any(len(clients) > 1 for clients in frameworks.values())

    def label(report: dict) -> str:
        parts = [stack(report, clients_vary)]
        for field in varying:
            value = report["config"].get(field)
            parts.append(f"{value} in flight" if field == "calls_in_flight_per_connection" else str(value))
        return " · ".join(parts)

    return label


def status(report: dict) -> str | None:
    """Why a row is no framework result, or None for a measured one. A client process's own report, which launch.py
    has not merged with its siblings' yet, holds a share of the configuration."""
    config = report["config"]
    if (config.get("processes") or 1) > 1 and report.get("domains") is None:
        return f"unmerged: client process {config.get('process')} of {config['processes']}"
    if report.get("unsupported"):
        return f"unsupported: {report['unsupported']}"
    if report.get("void_reason"):
        return f"void: {report['void_reason']}"
    if report.get("client_limited"):
        return "client-limited"
    if report.get("errors"):
        return f"errors: {report['errors']}"
    return None


def closed_loop(report: dict) -> bool:
    """Whether the client sent each call as the last returned, so calls/s is the stack's capacity, not a set rate."""
    return not report["config"].get("offered_calls_per_second")


def plaintext_steady(report: dict) -> bool:
    """Whether the row measured plaintext calls over open connections, the transport every stack shares."""
    return report["config"]["security"] == "none" and report["config"]["phase"] == "steady"


def gigabits_per_card(report: dict, direction: str) -> tuple[float | None, str]:
    """The row's Gbit/s per server card in `direction` and where it came from: the recorder's card counters when the
    row was recorded, else the body bytes over the window shared by the client's interfaces, which leaves out
    headers and framing."""
    cards = report.get("network_interfaces", {}).get("server")
    if cards:
        return sum(server_cards(report, direction).values()) / len(cards), "card counters"
    body = report["request_body_bytes" if direction == "received" else "response_body_bytes"]
    interfaces = report["config"].get("interfaces")
    if not interfaces or not report.get("seconds"):
        return None, "unrecorded"
    return body * 8 / report["seconds"] / 1e9 / interfaces, "body bytes"


def spell_bytes(count: int) -> str:
    for unit, size in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if count >= size and count % size == 0:
            return f"{count // size:,} {unit}"
    return f"{count:,} B"


def split(rows: list[dict], key) -> dict:
    groups: dict = {}
    for row in rows:
        groups.setdefault(key(row), []).append(row)
    return dict(sorted(groups.items(), key=lambda item: tuple(str(part) for part in item[0])))


class Chart:
    """One SVG: its rows, drawn as (series, x, value, row), and the matching rows it leaves out."""

    def __init__(self, stem: str, title: str, rows: list[dict]):
        self.stem, self.title, self.rows = stem, title, rows
        self.drawn: list[tuple[str, object, float, dict]] = []
        self.notes: list[str] = []

    def measured(self) -> list[dict]:
        return [row for row in self.rows if status(row) is None]

    def left_out(self) -> list[dict]:
        return [row for row in self.rows if status(row) is not None]


def footnote(figure, chart: Chart) -> None:
    """Names under the figure the stacks of every row it leaves out, by reason; the sidecar lists each row."""
    import textwrap

    reasons: dict[str, set[str]] = {}
    for row in chart.left_out():
        reasons.setdefault(status(row).partition(":")[0], set()).add(stack(row))
    if reasons:
        named = "; ".join(f"{reason}: {', '.join(sorted(stacks))}" for reason, stacks in sorted(reasons.items()))
        text = f"Left out, with every row and reason in {chart.stem}.md: {named}"
        figure.text(0.01, -0.02, textwrap.fill(text, 170), ha="left", va="top", fontsize=9, alpha=0.8)


def finish(plt, figure, chart: Chart, output: Path, machine_name: str | None) -> Path | None:
    """Saves the chart and its sidecar, or nothing when every matching row was left out, saying which."""
    if not chart.drawn:
        plt.close(figure)
        print(f"- {chart.stem}: not drawn, every one of its {len(chart.rows)} rows was left out", file=sys.stderr)
        return None
    footnote(figure, chart)
    path = output / f"{chart.stem}.svg"
    figure.savefig(path, format="svg", bbox_inches="tight")
    plt.close(figure)
    write_sidecar(path, chart, machine_name)
    print(path)
    return path


def describe_config(rows: list[dict]) -> tuple[list[str], list[str]]:
    """The config fields every row shares, as `name: value`, and the names of those that differ."""
    names = sorted({name for row in rows for name in row["config"]} - {"process", "threads", "seed"})
    shared, varying = [], []
    for name in names:
        values = {json.dumps(row["config"].get(name)) for row in rows}
        (shared if len(values) == 1 else varying).append(name)
    return [f"{name}: {cell(rows[0]['config'].get(name))}" for name in shared], varying


def write_sidecar(svg: Path, chart: Chart, machine_name: str | None) -> None:
    """`<figure>.md` beside the SVG: the rows drawn, the configuration and machine, and the rows left out."""
    every = [row for _, _, _, row in chart.drawn] + chart.left_out()
    lines = [f"# {chart.title}", "", f"- Figure: `{svg}`"]
    if every:
        machine = every[0]["machine"]
        name = machine_name or machine.get("cpu_name") or machine.get("cpu_model")
        lines.append(
            f"- Client box, as rows record it: {name}, {cell(machine.get('logical_cores'))} logical cores, "
            f"{cell(machine.get('ram_bytes'))} bytes of RAM, kernel {machine.get('kernel')}, {machine.get('os')}; "
            f"client cores {machine.get('budget', {}).get('cpus')}"
        )
        lists = sorted({str(row.get("server_cpu_list")) for row in every})
        lines.append(f"- Server cores, per row: {', '.join(lists)}")
        shared, varying = describe_config(every)
        lines.append("- Configuration every row shares: " + ("; ".join(shared) or "none"))
        lines.append("- Configuration that varies: " + (", ".join(varying) or "none"))
    lines += [f"- {note}" for note in chart.notes]
    lines += ["", "## Rows Drawn", "", "| Series | X | Value | Row |", "| :----- | -: | ----: | :-- |"]
    for series, x, value, row in chart.drawn:
        lines.append(f"| {series} | {cell(x)} | {cell(value)} | `{row.get('source')}` |")
    if chart.left_out():
        label = series_of(chart.rows)
        lines += ["", "## Rows Left Out", "", "| Series | Connections | Reason | Row |", "| :-- | --: | :-- | :-- |"]
        for row in chart.left_out():
            connections = cell(row["config"].get("connections"))
            lines.append(f"| {label(row)} | {connections} | {status(row)} | `{row.get('source')}` |")
    svg.with_suffix(".md").write_text("\n".join(lines) + "\n")


def machine_title(rows: list[dict], machine_name: str | None) -> str:
    machine = rows[0]["machine"]
    return machine_name or machine.get("cpu_name") or machine.get("cpu_model") or ""


def emphasis(series: str) -> float:
    """UCall's series drawn opaque, every other stack's half transparent, as `plot` draws them."""
    return 1.0 if series.startswith("ucall") else 0.55


def draw_lines(axis, chart: Chart, x, value, label, suffix: str = "") -> None:
    """One line per series over `x`, every point recorded in `chart.drawn` with `suffix` after its series."""
    lines = split(chart.measured(), lambda row: (label(row),))
    for index, ((series,), points) in enumerate(lines.items()):
        points = sorted((x(row), value(row), row) for row in points if value(row) is not None and x(row) is not None)
        if not points:
            continue
        chart.drawn += [(series + suffix, px, py, row) for px, py, row in points]
        axis.plot(
            [px for px, _, _ in points],
            [py for _, py, _ in points],
            marker=MARKERS[index % len(MARKERS)],
            linestyle=LINE_STYLES[index // len(MARKERS) % len(LINE_STYLES)],
            color=THEME["ink"],
            alpha=emphasis(series),
            label=series,
        )
        # Every line named at its last point too, since the lines share one ink.
        axis.annotate(
            series,
            points[-1][:2],
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
            alpha=emphasis(series),
        )


def power_of_two_axis(axis, values) -> None:
    axis.set_xscale("log", base=2)
    axis.set_xticks(sorted(values))
    axis.set_xticklabels([cell(value) for value in sorted(values)])
    axis.minorticks_off()


def draw_bars(axis, chart: Chart, categories, groups, cell_of, horizontal=False, logarithmic=False) -> None:
    """Grouped bars: one group of bars per category, one bar per group, from `cell_of(category, group)` returning
    (value, row) for a measured row, (None, row) for one left out, labelled in its slot, or (None, None). A
    `logarithmic` axis suits bars of stacks whose values differ by orders of magnitude."""
    width = 0.8 / max(len(groups), 1)
    cells = {(category, group): cell_of(category, group) for category in categories for group in groups}
    values = [value for value, _ in cells.values() if value]
    low, top = min(values, default=1), max(values, default=1)
    # Left-out slots and the bars' feet sit at the axis floor: zero, or half the lowest bar on a logarithmic axis.
    floor = low / 2 if logarithmic else 0
    bar = axis.barh if horizontal else axis.bar
    for position, group in enumerate(groups):
        offsets = [index - 0.4 + width * (position + 0.5) for index in range(len(categories))]
        slots = [cells[(category, group)] for category in categories]
        lengths = [value - floor if value else 0 for value, _ in slots]
        place = {"left": floor} if horizontal else {"bottom": floor}
        bar(
            offsets,
            lengths,
            width * 0.9,
            color=THEME["ink"],
            hatch=HATCHES[position % len(HATCHES)],
            alpha=0.55 + 0.45 * (position == len(groups) - 1),
            label=group,
            **place,
        )
        for offset, category, (value, row) in zip(offsets, categories, slots):
            if value is not None:
                chart.drawn.append((f"{category} · {group}", group, value, row))
            if row is None:
                continue
            text = cell(value) if value is not None else (status(row) or "not recorded").partition(":")[0]
            point = (value or floor, offset) if horizontal else (offset, value or floor)
            axis.annotate(
                text,
                point,
                fontsize=8,
                rotation=0 if horizontal else 90,
                xytext=(3, 0) if horizontal else (0, 3),
                textcoords="offset points",
                ha="left" if horizontal else "center",
                va="center" if horizontal else "bottom",
            )
    ticks = range(len(categories))
    # Head room above the tallest bar for its rotated label: a quarter more, or a decade on a logarithmic axis.
    limits = (floor, top * 10) if logarithmic else (0, top * 1.25)
    if horizontal:
        axis.set_yticks(ticks, categories)
        axis.set_xscale("log" if logarithmic else "linear")
        axis.set_xlim(*limits)
    else:
        axis.set_xticks(ticks, categories, rotation=20, ha="right")
        axis.set_yscale("log" if logarithmic else "linear")
        axis.set_ylim(*limits)


def figure_bandwidth(plt, rows, output, machine_name, ceilings) -> list[Path]:
    """F1: Gbit/s per server card against body size, per stack, one chart per workload, test, connection count and
    cores per interface, with each `--ceiling` drawn across."""
    selected = [row for row in rows if row["config"]["workload"] in WORKLOAD_DIRECTIONS and closed_loop(row)]
    selected = [row for row in selected if plaintext_steady(row)]
    key = lambda row: (row["config"]["workload"], test_of(row), row["config"]["connections"], cores_per_interface(row))
    paths = []
    for (workload, test, connections, cores), group in split(selected, key).items():
        direction = WORKLOAD_DIRECTIONS[workload]
        stem = f"f1-{workload}-{test}-{connections}-connections-{cores}-cores-per-interface"
        title = f"Gbit/s per card {direction} by the server, {workload}, {test} test, {connections:,} connections, "
        chart = Chart(stem, title + spell_cores(cores), group)
        sources = {gigabits_per_card(row, direction)[1] for row in chart.measured()}
        chart.notes.append(f"Gbit/s per card from: {', '.join(sorted(sources)) or 'nothing measured'}")
        figure, axis = plt.subplots(figsize=(16, 9.2))
        label = series_of(group)
        draw_lines(
            axis,
            chart,
            lambda row: row["config"]["body_bytes"],
            lambda row, direction=direction: gigabits_per_card(row, direction)[0],
            label,
        )
        for index, (name, gigabits) in enumerate(ceilings):
            axis.axhline(gigabits, color=THEME["frame"], linestyle=LINE_STYLES[1 + index % 3], linewidth=1)
            axis.annotate(
                f"{name} {gigabits:,.0f} Gbit/s",
                (1, gigabits),
                xycoords=("axes fraction", "data"),
                ha="right",
                va="bottom",
                fontsize=10,
            )
            chart.notes.append(f"Ceiling `{name}`: {gigabits:,.1f} Gbit/s per card, from --ceiling")
        sizes = sorted({row["config"]["body_bytes"] for row in group})
        power_of_two_axis(axis, sizes)
        axis.set_xticklabels([spell_bytes(size) for size in sizes])
        axis.set_ylim(bottom=0)
        axis.set_title(f"{chart.title}\n{machine_title(group, machine_name)}", loc="left")
        axis.set_xlabel("Body size")
        axis.set_ylabel(f"Gbit/s per card, {direction}")
        axis.grid(alpha=0.2)
        axis.legend(loc="best", frameon=False)
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def figure_scaling(plt, rows, output, machine_name, prefix="f2", subject="Tool calls") -> list[Path]:
    """F2: calls/s per server core against server cores per interface, per stack, one chart per test and
    connection count, from the steady tool rows."""
    selected = [row for row in rows if row["config"]["workload"] == "tool" and cores_per_interface(row) is not None]
    selected = [row for row in selected if closed_loop(row) and plaintext_steady(row)]
    paths = []
    for (test, connections), group in split(selected, lambda row: (test_of(row), row["config"]["connections"])).items():
        stem = f"{prefix}-{test}-{connections}-connections"
        chart = Chart(stem, f"{subject}: calls/s per server core, {test} test, {connections:,} connections", group)
        figure, axis = plt.subplots(figsize=(16, 9.2))
        draw_lines(axis, chart, cores_per_interface, per_server_core, series_of(group))
        power_of_two_axis(axis, {cores_per_interface(row) for row in group})
        axis.set_yscale("log")
        axis.set_title(f"{chart.title}\n{machine_title(group, machine_name)}", loc="left")
        axis.set_xlabel("Server cores per network interface")
        axis.set_ylabel("calls/s per server core")
        axis.grid(alpha=0.2)
        axis.legend(loc="best", frameon=False)
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def figure_latency_under_load(plt, rows, output, machine_name) -> list[Path]:
    """F3: p50, p99 and p999 against the offered calls/s of open-loop rows, one panel per percentile."""
    selected = [row for row in rows if row["config"].get("offered_calls_per_second")]
    key = lambda row: (row["config"]["workload"], row["config"]["connections"], cores_per_interface(row))
    paths = []
    for (workload, connections, cores), group in split(selected, key).items():
        stem = f"f3-{workload}-{connections}-connections-{cores}-cores-per-interface"
        title = f"Latency against offered load, {workload}, {connections:,} connections, {spell_cores(cores)}"
        chart = Chart(stem, title, group)
        figure, axes = plt.subplots(1, len(PERCENTILES), figsize=(20, 7.5), sharey=True)
        label = series_of(group)
        for axis, percentile in zip(axes, PERCENTILES):
            draw_lines(
                axis,
                chart,
                lambda row: row["config"]["offered_calls_per_second"],
                lambda row, name=percentile: row["latency_us"][name],
                label,
                f" · {percentile}",
            )
            axis.set_xscale("log")
            axis.set_yscale("log")
            axis.set_title(percentile, loc="left")
            axis.set_xlabel("Offered calls/s")
            axis.grid(alpha=0.2)
        axes[0].set_ylabel("Latency, µs")
        axes[0].legend(loc="upper left", frameon=False, fontsize=9)
        figure.suptitle(f"{title}\n{machine_title(group, machine_name)}", x=0.01, ha="left")
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def keyed(rows: list[dict], key) -> dict:
    """The row of each key, the first measured one where several share it."""
    found: dict = {}
    for row in sorted(rows, key=lambda row: (status(row) is not None, row.get("source", ""))):
        found.setdefault(key(row), row)
    return found


def bar_cell(found: dict, value):
    """`draw_bars`' cell function over rows keyed by (category, group)."""

    def cell_of(category, group):
        row = found.get((category, group))
        if row is None:
            return None, None
        return (value(row) if status(row) is None else None), row

    return cell_of


def figure_security(plt, rows, output, machine_name) -> list[Path]:
    """F4: handshakes/s per server core from the handshake phase, and server CPU per call from the steady phase,
    per stack and security mode."""
    selected = [row for row in rows if row["config"]["workload"] == "tool" and test_of(row) == "latency"]
    selected = [row for row in selected if closed_loop(row)]
    paths = []
    for (connections, cores), group in split(
        selected, lambda r: (r["config"]["connections"], cores_per_interface(r))
    ).items():
        if len({row["config"]["security"] for row in group}) < 2:
            continue  # one security mode: nothing to compare
        stem = f"f4-security-{connections}-connections-{cores}-cores-per-interface"
        chart = Chart(stem, f"Security cost, tool calls, {connections:,} connections, {spell_cores(cores)}", group)
        stacks = sorted({stack(row) for row in group})
        modes = [mode for mode in SECURITY_MODES if any(row["config"]["security"] == mode for row in group)]
        figure, axes = plt.subplots(1, 2, figsize=(20, 8))
        panels = (
            ("handshake", "Handshakes/s per server core", per_server_core),
            ("steady", "Server CPU per call, µs", lambda row: first_of(row, "server_cpu_microseconds_per_call")),
        )
        for axis, (phase, ylabel, value) in zip(axes, panels):
            phased = [row for row in group if row["config"]["phase"] == phase]
            found = keyed(phased, lambda row: (stack(row), row["config"]["security"]))
            draw_bars(axis, chart, stacks, modes, bar_cell(found, value), logarithmic=True)
            axis.set_title(f"{phase.capitalize()} phase", loc="left")
            axis.set_ylabel(ylabel)
            axis.grid(alpha=0.2, axis="y")
        axes[0].legend(loc="upper right", frameon=False)
        figure.suptitle(f"{chart.title}\n{machine_title(group, machine_name)}", x=0.01, ha="left")
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def figure_batching(plt, rows, output, machine_name) -> list[Path]:
    """F5: calls/s per server core and p99 against the server's batch size, per stack; stacks that do not batch
    appear as their unbatched one-call-per-connection rows, drawn across."""
    batched = [row for row in rows if row["config"].get("batch_size") is not None and closed_loop(row)]
    key = lambda row: (row["config"]["workload"], row["config"]["connections"], cores_per_interface(row))
    paths = []
    for (workload, connections, cores), group in split(batched, key).items():
        references = [
            row
            for row in rows
            if row["config"].get("batch_size") is None
            and key(row) == (workload, connections, cores)
            and row["config"]["variant"] == "plain"
            and test_of(row) == "latency"
            and status(row) is None
            and closed_loop(row)
            and plaintext_steady(row)
        ]
        references = list(keyed(references, stack).values())  # one unbatched row per stack
        stem = f"f5-batching-{workload}-{connections}-connections-{cores}-cores-per-interface"
        chart = Chart(stem, f"Batching, {workload}, {connections:,} connections, {spell_cores(cores)}", group)
        figure, axes = plt.subplots(1, 2, figsize=(20, 8))
        label = series_of(group)
        panels = (("calls/s per server core", per_server_core), ("p99 latency, µs", lambda r: r["latency_us"]["p99"]))
        for axis, (ylabel, value) in zip(axes, panels):
            draw_lines(axis, chart, lambda row: row["config"]["batch_size"], value, label)
            for index, row in enumerate(references):
                axis.axhline(value(row), color=THEME["frame"], linestyle=LINE_STYLES[1 + index % 3], linewidth=1)
                axis.annotate(
                    f"{stack(row)} unbatched",
                    (1, value(row)),
                    xycoords=("axes fraction", "data"),
                    ha="right",
                    va="bottom",
                    fontsize=9,
                )
                chart.drawn.append((f"{stack(row)} unbatched", "-", value(row), row))
            power_of_two_axis(axis, {row["config"]["batch_size"] for row in group})
            axis.set_yscale("log")
            axis.set_xlabel("Server batch size, calls")
            axis.set_ylabel(ylabel)
            axis.grid(alpha=0.2)
        axes[0].legend(loc="best", frameon=False)
        figure.suptitle(f"{chart.title}\n{machine_title(group, machine_name)}", x=0.01, ha="left")
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def upstream_of(report: dict) -> str | None:
    """The program a fan-out row's upstreams ran, from the `serveval-upstream-<name>-...` units launch.py named in
    the `upstreams-<connections>.log` beside the row, or None where no such log is."""
    import re

    log = Path(report.get("source", "")).with_name(f"upstreams-{report['config']['connections']}.log")
    if not log.exists():
        return None
    found = re.search(r"serveval-upstream-([A-Za-z0-9.]+)-\d+-\d+", log.read_text(errors="replace"))
    return found.group(1) if found else None


def figure_pairs(plt, rows, output, machine_name) -> list[Path]:
    """F6: a client × server grid of each half's CPU per tool call where servers were measured with several clients,
    and the fan-out rows side by side: the server's own client calling the same upstreams."""
    from matplotlib.colors import LinearSegmentedColormap

    paths = []
    plain = [row for row in rows if test_of(row) == "latency" and closed_loop(row) and plaintext_steady(row)]
    tools = [row for row in plain if row["config"]["workload"] == "tool"]
    key = lambda row: (row["config"]["connections"], cores_per_interface(row))
    for (connections, cores), group in split(tools, key).items():
        # The grid keeps servers measured with several clients and clients measured against several servers.
        clients_of, servers_of = {}, {}
        for row in group:
            clients_of.setdefault(stack(row), set()).add(row["config"]["client"])
            servers_of.setdefault(row["config"]["client"], set()).add(stack(row))
        servers = sorted(server for server, met in clients_of.items() if len(met) > 1)
        clients = sorted(client for client, met in servers_of.items() if len(met) > 1)
        group = [row for row in group if stack(row) in servers and row["config"]["client"] in clients]
        if not servers or not clients:
            continue  # every server met one client: no grid to draw
        stem = f"f6-grid-{connections}-connections-{cores}-cores-per-interface"
        chart = Chart(stem, f"Client × server, tool calls, {connections:,} connections, {spell_cores(cores)}", group)
        found = keyed(group, lambda row: (stack(row), row["config"]["client"]))
        colors = LinearSegmentedColormap.from_list("theme", [THEME["field"], THEME["ink"]])
        figure, axes = plt.subplots(1, 2, figsize=(20, 8))
        halves = (
            ("Server CPU per call, µs", "server_cpu_microseconds_per_call"),
            ("Client CPU per call, µs", "client_cpu_microseconds_per_call"),
        )
        for axis, (title, field) in zip(axes, halves):
            grid = [[None] * len(clients) for _ in servers]
            for (server, client), row in found.items():
                if status(row) is None and row.get(field) is not None:
                    grid[servers.index(server)][clients.index(client)] = row[field]
                    chart.drawn.append((f"{server} ← {client} · {field}", client, row[field], row))
            values = [value for line in grid for value in line if value is not None]
            top = max(values, default=1)
            axis.imshow([[value or 0 for value in line] for line in grid], cmap=colors, vmin=0, vmax=top, aspect="auto")
            for (server, client), row in found.items():
                value = grid[servers.index(server)][clients.index(client)]
                text = cell(value) if value is not None else (status(row) or "not recorded").partition(":")[0]
                dark = value is not None and value > top / 2
                axis.text(
                    clients.index(client),
                    servers.index(server),
                    text,
                    ha="center",
                    va="center",
                    color=THEME["field"] if dark else THEME["ink"],
                    fontsize=11,
                )
            axis.set_xticks(range(len(clients)), clients)
            axis.set_yticks(range(len(servers)), servers)
            axis.set_xlabel("Client")
            axis.set_ylabel("Server")
            axis.set_title(title, loc="left")
        figure.suptitle(f"{chart.title}\n{machine_title(group, machine_name)}", x=0.01, ha="left")
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    fanouts = [row for row in plain if row["config"]["workload"] == "fanout"]
    for (connections, cores), group in split(fanouts, key).items():
        stem = f"f6-fanout-{connections}-connections-{cores}-cores-per-interface"
        width = sorted({row["config"].get("width") for row in group}, key=str)
        title = f"Fan-out of {', '.join(map(cell, width))} upstream calls, {connections:,} connections, "
        chart = Chart(stem, title + spell_cores(cores), group)
        chart.notes.append(
            "Each server calls its upstreams with its own client; UCall's client shares the server's "
            "event loop. Categories name the upstreams' program, from the upstreams log beside each row"
        )

        series = series_of(group)

        def label(row: dict, series=series) -> str:
            return f"{series(row)} → {upstream_of(row) or 'unlogged'} upstreams"

        stacks = sorted({label(row) for row in group})
        found = keyed(group, lambda row, label=label: (label(row), "fan-out"))
        figure, axes = plt.subplots(1, 3, figsize=(22, 8))
        panels = (
            ("calls/s per server core", per_server_core),
            ("Server CPU per fan-out call, µs", lambda row: first_of(row, "server_cpu_microseconds_per_call")),
            ("p99 latency, µs", lambda row: row["latency_us"]["p99"]),
        )
        for axis, (ylabel, value) in zip(axes, panels):
            draw_bars(axis, chart, stacks, ["fan-out"], bar_cell(found, value), logarithmic=True)
            axis.set_title(ylabel, loc="left")
            axis.grid(alpha=0.2, axis="y")
        figure.suptitle(f"{chart.title}\n{machine_title(group, machine_name)}", x=0.01, ha="left")
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def figure_memory(plt, rows, output, machine_name) -> list[Path]:
    """F7: server KB per connection at the largest connection count measured, per stack and cores per interface,
    from the latency test, one chart per workload."""
    selected = [row for row in rows if test_of(row) == "latency" and cores_per_interface(row) is not None]
    selected = [row for row in selected if closed_loop(row) and plaintext_steady(row)]
    if not selected:
        return []
    most = max(row["config"]["connections"] for row in selected)
    selected = [row for row in selected if row["config"]["connections"] == most]
    paths = []
    for (workload,), group in split(selected, lambda row: (row["config"]["workload"],)).items():
        stem = f"f7-memory-{workload}-{most}-connections"
        chart = Chart(stem, f"Server memory per connection at {most:,} connections, {workload} calls", group)
        stacks = sorted({stack(row) for row in group}, reverse=True)
        cores = [spell_cores(count) for count in sorted({cores_per_interface(row) for row in group})]
        found = keyed(group, lambda row: (stack(row), spell_cores(cores_per_interface(row))))
        kilobytes = lambda row: first_of(row, "server_kilobytes_per_connection", "server_kb_per_connection")
        figure, axis = plt.subplots(figsize=(16, 9.2))
        draw_bars(axis, chart, stacks, cores, bar_cell(found, kilobytes), horizontal=True)
        axis.set_xlabel("Server KB per connection: memory peak under load over the idle peak, per connection")
        axis.set_title(f"{chart.title}\n{machine_title(group, machine_name)}", loc="left")
        axis.grid(alpha=0.2, axis="x")
        handles, labels = axis.get_legend_handles_labels()
        # Horizontal groups stack their first bar lowest, so the legend lists them bottom up to match.
        axis.legend(handles[::-1], labels[::-1], loc="lower right", frameon=False)
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def step_of(report: dict) -> int | None:
    """The UCall step a row measured, from a `step-N` directory in its path."""
    for part in Path(report.get("source", "")).parts:
        if part.startswith("step-") and part.removeprefix("step-").isdigit():
            return int(part.removeprefix("step-"))
    return None


def figure_steps(plt, rows, output, machine_name) -> list[Path]:
    """F9: UCall after each step, one bar per language and cores per interface: calls/s per server core for tool and
    fan-out calls, Gbit/s per card for uploads and downloads."""
    selected = [row for row in rows if step_of(row) is not None and closed_loop(row) and plaintext_steady(row)]
    key = lambda row: (row["config"]["workload"], test_of(row), row["config"]["variant"], row["config"]["connections"])
    paths = []
    for (workload, test, variant, connections), group in split(selected, key).items():
        direction = WORKLOAD_DIRECTIONS.get(workload)
        if direction:
            ylabel, value = f"Gbit/s per card, {direction}", lambda row, d=direction: gigabits_per_card(row, d)[0]
        else:
            ylabel, value = "calls/s per server core", per_server_core
        stem = f"f9-steps-{workload}-{test}-{variant}-{connections}-connections"
        chart = Chart(
            stem, f"UCall steps: {ylabel}, {workload}, {test} test, {variant}, {connections:,} connections", group
        )
        if direction:
            sources = {gigabits_per_card(row, direction)[1] for row in chart.measured()}
            chart.notes.append(f"Gbit/s per card from: {', '.join(sorted(sources)) or 'nothing measured'}")
        steps = [f"step {step}" for step in sorted({step_of(row) for row in group})]
        bars = sorted({(row["config"]["language"], cores_per_interface(row)) for row in group}, key=str)
        groups = [f"{language}, {spell_cores(cores)}" for language, cores in bars]
        found = keyed(
            group,
            lambda row: (
                f"step {step_of(row)}",
                f"{row['config']['language']}, {spell_cores(cores_per_interface(row))}",
            ),
        )
        figure, axis = plt.subplots(figsize=(16, 9.2))
        draw_bars(axis, chart, steps, groups, bar_cell(found, value))
        axis.set_xticks(range(len(steps)), steps, rotation=0, ha="center")
        axis.set_ylabel(ylabel)
        axis.set_title(f"{chart.title}\n{machine_title(group, machine_name)}", loc="left")
        axis.grid(alpha=0.2, axis="y")
        axis.legend(loc="upper left", frameon=False)
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def figure_python(plt, rows, output, machine_name) -> list[Path]:
    """F10: the Python stacks, as F2 draws them, and their p99 per cores per interface on the latency test."""
    python = [row for row in rows if row["config"]["language"] == "python"]
    paths = figure_scaling(plt, python, output, machine_name, prefix="f10-scaling", subject="Python tool calls")
    selected = [row for row in python if row["config"]["workload"] == "tool" and test_of(row) == "latency"]
    selected = [row for row in selected if closed_loop(row) and plaintext_steady(row)]
    for (connections,), group in split(selected, lambda row: (row["config"]["connections"],)).items():
        chart = Chart(
            f"f10-p99-{connections}-connections", f"Python tool calls: p99, {connections:,} connections", group
        )
        stacks = sorted({stack(row) for row in group})
        cores = [spell_cores(count) for count in sorted({cores_per_interface(row) for row in group}, key=str)]
        found = keyed(group, lambda row: (stack(row), spell_cores(cores_per_interface(row))))
        figure, axis = plt.subplots(figsize=(16, 9.2))
        draw_bars(axis, chart, stacks, cores, bar_cell(found, lambda row: row["latency_us"]["p99"]), logarithmic=True)
        axis.set_ylabel("p99 latency, µs")
        axis.set_title(f"{chart.title}\n{machine_title(group, machine_name)}", loc="left")
        axis.grid(alpha=0.2, axis="y")
        axis.legend(loc="upper left", frameon=False)
        paths += filter(None, [finish(plt, figure, chart, output, machine_name)])
    return paths


def plot_figures(rows: list[dict], names: list[str], output: Path, machine_name: str | None, ceilings) -> None:
    """Every figure in `names` from `rows`, measured or not: each figure draws its measured rows and lists the
    unsupported, void and client-limited ones it leaves out."""
    plt = themed_pyplot()
    output.mkdir(parents=True, exist_ok=True)
    drawers = {
        "f1": lambda: figure_bandwidth(plt, rows, output, machine_name, ceilings),
        "f2": lambda: figure_scaling(plt, rows, output, machine_name),
        "f3": lambda: figure_latency_under_load(plt, rows, output, machine_name),
        "f4": lambda: figure_security(plt, rows, output, machine_name),
        "f5": lambda: figure_batching(plt, rows, output, machine_name),
        "f6": lambda: figure_pairs(plt, rows, output, machine_name),
        "f7": lambda: figure_memory(plt, rows, output, machine_name),
        "f9": lambda: figure_steps(plt, rows, output, machine_name),
        "f10": lambda: figure_python(plt, rows, output, machine_name),
    }
    for name in names:
        if not drawers[name]():
            print(f"- {name}: no rows to draw", file=sys.stderr)


def parse_ceiling(text: str) -> tuple[str, float]:
    name, _, gigabits = text.partition("=")
    try:
        return name, float(gigabits)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text}: expected NAME=GBITS, like iperf3=173.4") from None


def parse_figures(text: str) -> list[str]:
    names = list(FIGURES) if text == "all" else [name.strip() for name in text.split(",")]
    unknown = [name for name in names if name not in FIGURES]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown figures {', '.join(unknown)}; choose from {', '.join(FIGURES)}")
    return names


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.partition("\n")[0])
    parser.add_argument("reports", type=Path, nargs="+", help="Reports, or directories searched for *.json.")
    parser.add_argument("--output", type=Path, help="Directory for SVG charts; without it, print Markdown.")
    parser.add_argument(
        "--records",
        type=Path,
        action="append",
        default=[],
        help="A scripts/record.sh directory per machine, like records-server, holding one subdirectory per run.",
    )
    parser.add_argument("--json-output", type=Path, help="Writes every row with its network interface figures.")
    parser.add_argument("--machine-name", help="The CPU's name in the Machine column, the report's CPU model if unset.")
    parser.add_argument(
        "--figures",
        type=parse_figures,
        help=f"Article figures to draw into --output, comma-separated from {', '.join(FIGURES)}, or all.",
    )
    parser.add_argument(
        "--ceiling",
        type=parse_ceiling,
        action="append",
        default=[],
        metavar="NAME=GBITS",
        help="A per-card ceiling drawn across the bandwidth figure, like iperf3=173.4; repeatable.",
    )
    args = parser.parse_args()
    found = read_reports(args.reports)
    if not found:
        parser.error("No reports found")
    records = read_records(args.records)
    # The server's per-core samples, one run per recorder directory, from the records directory named for the server.
    cpu_records = [
        read_cpu(path)
        for directory in args.records
        if directory.name.removeprefix("records-") == "server"
        for path in sorted([*directory.glob("cpu.log"), *directory.glob("*/cpu.log")])
    ]
    rows, voided = [], []
    for path, source in found:
        # A copy, so the figures never reach the report files themselves.
        row = dict(source, source=str(path))
        if records and not source.get("unsupported"):
            row["network_interfaces"] = network_interfaces(source, records)
            if not row["network_interfaces"]:
                row["void_reason"] = "unrecorded"
            for direction in ("received", "transmitted"):
                row[f"server_gigabits_per_second_{direction}_per_network_interface"] = server_cards(row, direction)
            row["server_interrupt_cpu_microseconds_per_call"] = interrupt_cpu_per_call(source, cpu_records)
        rows.append(row)
    unsupported = [row for row in rows if row.get("unsupported")]
    voided = [(row, row["void_reason"]) for row in rows if row.get("void_reason")]
    measured = [row for row in rows if not row.get("unsupported") and not row.get("void_reason")]
    framework = [row for row in measured if not row.get("client_limited")]
    limited = [row for row in measured if row.get("client_limited")]
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(rows, indent=1))
    if args.figures:
        if not args.output:
            parser.error("--figures draws into --output; name a directory")
        plot_figures(rows, args.figures, args.output, args.machine_name, args.ceiling)
        return
    if args.output:
        plot(framework, args.output)
        return
    print_results(framework, args.machine_name)
    if records:
        print_ceiling(measured)
    if limited:
        print("\n## Client-Limited\n")
        print("These rows were limited by their client and are not framework results.\n")
        print_table(sorted(limited, key=order), [("Test", test_of, ":--"), *COLUMNS], args.machine_name)
    if measured:
        print("\n## Client Cost\n")
        print_table(sorted(measured, key=order), CLIENT_COLUMNS, args.machine_name, settings=())
    if unsupported:
        print_unsupported(unsupported)
    if voided:
        print_voided(voided)


if __name__ == "__main__":
    main()
