# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Print ServEval JSON reports as one Markdown table per workload, or plot them as SVG with --output.

With --records, a report is void when an ENA allowance counter that `scripts/record.sh` sampled on either box rose
inside its window, or when the samples do not cover its window; void reports are listed apart and never plotted.
"""

import argparse
import json
from pathlib import Path

THEME = {
    "field": "#C44536",
    "ink": "#FFE6A7",
    "frame": "#E5E5E5",
    "font": ["Poppins", "Helvetica", "Arial", "sans-serif"],
}
WORKLOADS = ("tool", "context", "fanout")
SCHEMA_VERSION = 2
ALLOWANCES = (
    "bw_in_allowance_exceeded",
    "bw_out_allowance_exceeded",
    "pps_allowance_exceeded",
    "conntrack_allowance_exceeded",
    "linklocal_allowance_exceeded",
)
MARKERS = ["o", "s", "D", "^", "v", "P", "X", "*"]
COLUMNS = (
    ("Machine", lambda report: report["machine"]["cpu_model"], ":--"),
    ("Framework", lambda report: report["config"]["framework"], ":--"),
    ("Client", lambda report: report["config"]["client"], ":--"),
    ("Language", lambda report: report["config"]["language"], ":--"),
    ("Variant", lambda report: report["config"]["variant"], ":--"),
    ("Parsing", lambda report: report["config"]["parsing"], ":--"),
    ("Security", lambda report: report["config"]["security"], ":--"),
    ("Phase", lambda report: report["config"]["phase"], ":--"),
    ("Processes", lambda report: report["config"]["processes"], "--:"),
    ("Connections", lambda report: report["config"]["connections"], "--:"),
    ("Body", lambda report: report["config"]["body_bytes"], "--:"),
    ("calls/s", lambda report: report["calls_per_second"], "--:"),
    ("p50, µs", lambda report: report["latency_us"]["p50"], "--:"),
    ("p99, µs", lambda report: report["latency_us"]["p99"], "--:"),
    ("Body GB/s", lambda report: f"{report['body_gigabytes_per_second']:.3f}", "--:"),
    ("Server CPU, µs/call", lambda report: report["server_cpu_us_per_call"], "--:"),
    ("KB per connection", lambda report: report["server_kb_per_connection"], "--:"),
    ("Client CPU, %", lambda report: report["client_cpu_percent"], "--:"),
    ("Errors", lambda report: report["errors"], "--:"),
    ("Unsent", lambda report: report["unsent"], "--:"),
    ("Client-limited", lambda report: "yes" if report["client_limited"] else "no", ":--"),
)
METRICS = (
    ("throughput", "Calls / second", lambda report: report["calls_per_second"]),
    ("latency", "p99 latency (µs)", lambda report: report["latency_us"]["p99"]),
)


def cell(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.0f}" if value >= 100 else f"{value:.1f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def series(report: dict) -> str:
    config = report["config"]
    parts = [config["framework"], config["client"], config["language"], config["variant"], config["parsing"]]
    parts += [config["security"], config["phase"]]
    if config["workload"] == "context":
        parts.append(f"{config['body_bytes']:,} B")
    return " · ".join(parts)


def allowance_samples(directory: Path) -> list[list[tuple[int, dict[str, int]]]]:
    """Each card's `ethtool -S` samples that `scripts/record.sh` wrote, as Unix milliseconds and allowance counters."""
    cards = []
    for path in sorted(directory.glob("ethtool-*.log")):
        samples = []
        for line in path.read_text().splitlines():
            name, _, value = line.strip().partition(": ")
            if line.startswith("@"):
                samples.append((int(line[1:]), {}))
            elif samples and name in ALLOWANCES:
                samples[-1][1][name] = int(value)
        cards.append(samples)
    return cards


def void_reason(report: dict, cards: list[list[tuple[int, dict[str, int]]]]) -> str | None:
    """The first allowance counter that rose across the report's window on any card, `unrecorded` when the samples
    do not cover the window, or None for a valid report."""
    start, end = report["window_unix_ms"]
    for samples in cards:
        before = [counters for stamp, counters in samples if stamp <= start]
        after = [counters for stamp, counters in samples if stamp >= end]
        if not before or not after:
            return "unrecorded"
        for name in ALLOWANCES:
            if after[0].get(name, 0) > before[-1].get(name, 0):
                return name
    return None


def print_voided(voided: list[tuple[dict, str]]) -> None:
    print("\nVoided rows:\n")
    print("| Framework | Workload | Parsing | Security | Phase | Connections | Counter |")
    print("| :-------- | :------- | :------ | :------- | :---- | ----------: | :------ |")
    for report, reason in voided:
        config = report["config"]
        cells = (config["framework"], config["workload"], config["parsing"], config["security"], config["phase"])
        print("| " + " | ".join(cells) + f" | {cell(config['connections'])} | {reason} |")


def print_tables(reports: list[dict]) -> None:
    for workload in WORKLOADS:
        selected = [report for report in reports if report["config"]["workload"] == workload]
        if not selected:
            continue
        selected.sort(
            key=lambda report: (
                report["config"]["connections"],
                report["config"]["body_bytes"],
                report["config"]["phase"],
                -report["calls_per_second"],
            )
        )
        print(f"\n{workload.capitalize()} calls:\n")
        print("| " + " | ".join(title for title, _, _ in COLUMNS) + " |")
        print("| " + " | ".join(alignment for _, _, alignment in COLUMNS) + " |")
        for report in selected:
            print("| " + " | ".join(cell(value(report)) for _, value, _ in COLUMNS) + " |")


def plot(reports: list[dict], output: Path) -> None:
    import matplotlib

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
        }
    )
    output.mkdir(parents=True, exist_ok=True)
    for workload in WORKLOADS:
        selected = [report for report in reports if report["config"]["workload"] == workload]
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
            axis.set_title(f"{workload.capitalize()} calls\n{selected[0]['machine']['cpu_model']}", loc="left")
            axis.set_xlabel("Connections")
            axis.set_ylabel(label)
            axis.grid(alpha=0.2)
            axis.legend(loc="best", frameon=False)
            figure.savefig(output / f"{workload}-{name}.svg", format="svg", bbox_inches="tight")
            plt.close(figure)
    print(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, help="A report, or a directory of <framework>-<config-hash>.json.")
    parser.add_argument("--output", type=Path, help="Directory for SVG charts; without it, print Markdown.")
    parser.add_argument(
        "--records",
        type=Path,
        action="append",
        default=[],
        help="A scripts/record.sh directory, once per box; reports a throttling counter touched are voided.",
    )
    args = parser.parse_args()
    paths = sorted(args.reports.glob("*.json")) if args.reports.is_dir() else [args.reports]
    reports = []
    for path in paths:
        report = json.loads(path.read_text())
        if report.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"Unsupported report schema: {path}")
        reports.append(report)
    if not reports:
        parser.error("No reports found")
    cards = [samples for directory in args.records for samples in allowance_samples(directory)]
    reasons = [(report, void_reason(report, cards)) for report in reports]
    voided = [(report, reason) for report, reason in reasons if reason]
    reports = [report for report, reason in reasons if not reason]
    if args.output:
        plot(reports, args.output)
    else:
        print_tables(reports)
    if voided:
        print_voided(voided)


if __name__ == "__main__":
    main()
