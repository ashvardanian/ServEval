# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Draws the article's figures from the rows measured so far and writes INDEX.md and NUMBERS.md beside them.

Each figure reads the row directories `--rows` names, relative to `--run`, and draws whatever they hold; a directory
not measured yet is named as such, so the script reruns as rows land. INDEX.md maps each figure to its SVGs, row
directories, single-configuration client commands rebuilt from each row's config, the code's diff hashes, the machine
facts and the box settings. NUMBERS.md holds the headline numbers, each with the row file it comes from. Neither
counts unsupported, void, unmerged or client-limited rows as framework results.
"""

import argparse
import datetime
import hashlib
import json
import statistics
import subprocess
import sys
from pathlib import Path

import plot

# Each figure's row directories under --run, by the slot names of the measurement plan; --rows overrides any.
FIGURE_ROWS = {
    "f1": ["bandwidth"],
    "f2": ["latency", "throughput"],
    "f3": ["load"],
    "f4": ["security"],
    "f5": ["batching"],
    "f6": ["pairs", "latency"],
    "f7": ["latency"],
    "f9": ["ucall-steps"],
    "f10": ["latency", "throughput"],
}
# Figures whose values need the card counters `scripts/record.sh` sampled.
RECORDED = {"f1", "f9"}
FIGURE_TITLES = {
    "f1": "Gbit/s per card against body size, with the iperf3 and DPDK ceilings",
    "f2": "Tool calls: calls/s per server core against cores per network interface",
    "f3": "Latency against offered load: p50, p99, p999",
    "f4": "Security cost: Noise IK and XX against TLS 1.3",
    "f5": "Built-in batching: calls/s per core and p99 against batch size",
    "f6": "Client × server grid and fan-out through each server's own client",
    "f7": "Server memory per connection at the largest connection count",
    "f9": "UCall step by step",
    "f10": "Python stacks",
}


def run_text(command: list[str], cwd: Path | None = None) -> str:
    try:
        done = subprocess.run(command, cwd=cwd, capture_output=True, check=False, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable ({error})"
    return done.stdout.decode(errors="replace").strip() if done.returncode == 0 else "unavailable"


def tree_state(repository: Path, server: str | None) -> str:
    """`base <commit>, diff sha256 <hash>` of a tree's uncommitted diff, on this box and on `server` over ssh."""
    show = "git -C {0} rev-parse HEAD && git -C {0} diff HEAD --binary | sha256sum | cut -c1-16"
    states = [("this box", ["bash", "-c", show.format(repository)])]
    if server:
        remote = show.format(str(repository).replace(str(Path.home()), "~"))
        states.append((server, ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", server, remote]))
    spelled = []
    for where, command in states:
        lines = run_text(command).splitlines()
        state = f"base `{lines[0][:12]}`, diff sha256 `{lines[1]}`" if len(lines) == 2 else "unavailable"
        spelled.append(f"{where}: {state}")
    return "; ".join(spelled)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16] if path.exists() else "missing"


def client_program(config: dict, repository: Path) -> str:
    """The client a row's config names, as first-run.sh starts it: the Rust binary where `<client>_client.rs`
    exists and the row is Rust, else the Python script."""
    name = config["client"].partition(".")[0]
    if config["language"] == "rust" and (repository / f"{name}_client.rs").exists():
        return f"target/release/serv-eval-{name}-client"
    return f"<venv>/bin/python {name}_client.py"


def command(report: dict, repository: Path) -> str:
    """The single-configuration client command that measures `report`'s configuration, rebuilt from its config."""
    config = report["config"]
    depth = config["calls_in_flight_per_connection"]
    if config["calls_in_flight_per_connection_source"] != "flag":
        depth = "server-maximum"
    flags = [
        f"--workloads {config['workload']}",
        f"--variants {config['variant']}",
        f"--parsing {config['parsing']}",
        f"--phases {config['phase']}",
        f"--security {config['security']}",
        f"--connections {config['connections']}",
        f"--calls-in-flight-per-connection {depth}",
        f"--width {config['width']}",
        f"--seed {config['seed']}",
        f"--between-runs {config['between_runs']}",
    ]
    if config["workload"] in plot.WORKLOAD_DIRECTIONS:
        flags.append(f"--body-bytes {config['body_bytes']}")
    if config.get("offered_calls_per_second"):
        flags.append(f"--calls-per-second {config['offered_calls_per_second']}")
    if report.get("seconds"):
        flags.append(f"--time-limit {report['seconds']:g}s")
    environment = []
    if config.get("server_cores_per_network_interface") is not None:
        environment.append(
            f"SERVEVAL_SERVER_CORES_PER_NETWORK_INTERFACE={config['server_cores_per_network_interface']}"
        )
    if config.get("batch_size") is not None:
        environment.append(f"SERVEVAL_BATCH_SIZE={config['batch_size']} (server)")
    launch = f"launch.py --layout {'one' if config['processes'] == 1 else 'per-domain'}"
    return " ".join([*environment, launch, "--", client_program(config, repository), *flags])


def draw(
    figure: str, directories: list[Path], arguments: argparse.Namespace, rows_directory: Path
) -> tuple[list, list]:
    """Draws one figure through plot.py, returning its SVG paths and every row it read, with network figures."""
    rows_path = rows_directory / f"{figure}.json"
    line = ["uv", "run", "--quiet", "--script", str(Path(__file__).with_name("plot.py")), *map(str, directories)]
    line += ["--figures", figure, "--output", str(arguments.article / "figures"), "--json-output", str(rows_path)]
    if figure in RECORDED:
        for records in arguments.records:
            line += ["--records", str(records)]
    for name, gigabits in arguments.ceiling:
        line += ["--ceiling", f"{name}={gigabits}"]
    if arguments.machine_name:
        line += ["--machine-name", arguments.machine_name]
    done = subprocess.run(line, capture_output=True, text=True, check=False)
    if done.returncode != 0:
        print(f"- {figure}: plot.py exited {done.returncode}\n{done.stderr}", file=sys.stderr)
        return [], []
    svgs = [Path(text) for text in done.stdout.splitlines() if text.endswith(".svg")]
    return svgs, json.loads(rows_path.read_text()) if rows_path.exists() else []


def received_gigabits(path: Path) -> float | None:
    try:
        return json.loads(path.read_text())["end"]["sum_received"]["bits_per_second"] / 1e9
    except (ValueError, KeyError):
        return None


def sweep_cells(run: Path) -> list[tuple[str, dict[str, float], Path]]:
    """Every cell of the latest ceiling sweep, `<run>/slots/S2/<stamp>/cores-<N>-<mode>/`, as its name and the
    Gbit/s each card received over all of its `<card>-<process>.json` iperf3 processes."""
    stamps = sorted((run / "slots" / "S2").glob("*/"))
    cells = []
    by_cores = lambda path: (
        (int(path.name.split("-")[1]), path.name) if path.name.split("-")[1].isdigit() else (0, path.name)
    )
    for cell in sorted(stamps[-1].glob("cores-*"), key=by_cores) if stamps else []:
        cards: dict[str, float] = {}
        for path in cell.glob("*-*.json"):
            rate = received_gigabits(path)
            if rate is not None:
                card = path.stem.rpartition("-")[0]
                cards[card] = cards.get(card, 0.0) + rate
        if cards:
            cells.append((cell.name, cards, cell))
    return cells


def iperf3_ceiling(run: Path) -> tuple[float, str] | None:
    """The most Gbit/s per card iperf3 received with the server receiving: the best receive cell of the ceiling
    sweep, counting the card that moved less, or, before the sweep, the median of `<run>/ceiling/card*-*.json`."""
    receiving = [
        (min(cards.values()), name, cell) for name, cards, cell in sweep_cells(run) if name.endswith("-receive")
    ]
    if receiving:
        gigabits, name, cell = max(receiving)
        return gigabits, f"ceiling sweep cell `{name}`, the slower card's rate, `{cell}`"
    runs = sorted((run / "ceiling").glob("card*-*.json"))
    rates = [rate for rate in map(received_gigabits, runs) if rate is not None]
    if not rates:
        return None
    return statistics.median(rates), f"median of {len(rates)} runs, " + ", ".join(f"`{path}`" for path in runs)


def measured(rows: list[dict]) -> list[dict]:
    return [row for row in rows if plot.status(row) is None]


def best(rows: list[dict], value) -> dict | None:
    scored = [row for row in rows if value(row) is not None]
    return max(scored, key=value) if scored else None


def cite(row: dict) -> str:
    return f"`{row['source']}`"


def numbers_scaling(rows: list[dict]) -> list[str]:
    """calls/s per server core per stack, test and cores per interface, and UCall Rust against the best other."""
    lines = []
    tools = [
        r
        for r in measured(rows)
        if r["config"]["workload"] == "tool" and plot.closed_loop(r) and plot.plaintext_steady(r)
    ]
    tools = [r for r in tools if plot.per_server_core(r) is not None]
    keys = sorted({(plot.test_of(r), r["config"]["connections"], plot.cores_per_interface(r)) for r in tools}, key=str)
    for test, connections, cores in keys:
        group = [
            r
            for r in tools
            if (plot.test_of(r), r["config"]["connections"], plot.cores_per_interface(r)) == (test, connections, cores)
        ]
        lines.append(f"\n### Tool calls, {test} test, {connections:,} connections, {plot.spell_cores(cores)}\n")
        tops = {}
        for row in group:
            name = plot.stack(row)
            if name not in tops or plot.per_server_core(row) > plot.per_server_core(tops[name]):
                tops[name] = row
        for name, row in sorted(tops.items(), key=lambda item: -plot.per_server_core(item[1])):
            variant = f", {row['config']['variant']}, {row['config']['calls_in_flight_per_connection']} in flight"
            lines.append(
                f"- {name}{variant}: **{plot.per_server_core(row):,.0f} calls/s per server core**, "
                f"p99 {row['latency_us']['p99']:,.0f} µs, "
                f"server CPU {plot.cell(row.get('server_cpu_microseconds_per_call'))} µs/call — {cite(row)}"
            )
        ucall = tops.get("ucall rust")
        others = [row for name, row in tops.items() if not name.startswith("ucall")]
        rival = best(others, plot.per_server_core)
        if ucall and rival:
            ratio = plot.per_server_core(ucall) / plot.per_server_core(rival)
            lines.append(
                f"- UCall Rust against the best other stack, {plot.stack(rival)}: **{ratio:.2f}×** per server core"
            )
    return lines


def numbers_memory(rows: list[dict]) -> list[str]:
    selected = [
        r for r in measured(rows) if plot.test_of(r) == "latency" and plot.closed_loop(r) and plot.plaintext_steady(r)
    ]
    if not selected:
        return []
    most = max(r["config"]["connections"] for r in selected)
    lines = [f"\n### Server KB per connection at {most:,} connections, tool calls\n"]
    for row in sorted(
        (r for r in selected if r["config"]["connections"] == most and r["config"]["workload"] == "tool"),
        key=lambda r: (plot.stack(r), plot.cores_per_interface(r) or 0),
    ):
        kilobytes = row.get("server_kilobytes_per_connection")
        lines.append(
            f"- {plot.stack(row)}, {plot.spell_cores(plot.cores_per_interface(row))}: "
            f"**{plot.cell(kilobytes)} KB** — {cite(row)}"
        )
    limited = [
        r for r in rows if plot.status(r) and r["config"]["connections"] == most and r["config"]["workload"] == "tool"
    ]
    for row in limited:
        lines.append(
            f"- {plot.stack(row)}, {plot.spell_cores(plot.cores_per_interface(row))}: "
            f"left out, {plot.status(row)} — {cite(row)}"
        )
    return lines


def numbers_bandwidth(rows: list[dict], title: str) -> list[str]:
    """The most Gbit/s per card each stack moved per body-carrying workload and cores per interface."""
    lines = []
    selected = [
        r for r in measured(rows) if r["config"]["workload"] in plot.WORKLOAD_DIRECTIONS and plot.plaintext_steady(r)
    ]
    for workload in sorted({r["config"]["workload"] for r in selected}):
        direction = plot.WORKLOAD_DIRECTIONS[workload]
        lines.append(f"\n### {title}: Gbit/s per card {direction} by the server, {workload}\n")
        rate = lambda row, direction=direction: plot.gigabits_per_card(row, direction)[0]
        keys = sorted(
            {(plot.stack(r), plot.cores_per_interface(r) or 0) for r in selected if r["config"]["workload"] == workload}
        )
        for name, cores in keys:
            row = best(
                [
                    r
                    for r in selected
                    if r["config"]["workload"] == workload
                    and (plot.stack(r), plot.cores_per_interface(r) or 0) == (name, cores)
                ],
                rate,
            )
            if row:
                source = plot.gigabits_per_card(row, direction)[1]
                lines.append(
                    f"- {name}, {plot.spell_cores(cores)}: **{rate(row):,.1f} Gbit/s per card** at "
                    f"{plot.spell_bytes(row['config']['body_bytes'])} bodies, {plot.test_of(row)} test, "
                    f"from {source} — {cite(row)}"
                )
    return lines


def numbers_steps(rows: list[dict]) -> list[str]:
    """UCall's first and last step side by side, per language, cores per interface, workload and test."""
    lines = []
    selected = [r for r in measured(rows) if plot.step_of(r) is not None and plot.plaintext_steady(r)]
    keys = sorted(
        {
            (
                r["config"]["language"],
                plot.cores_per_interface(r),
                r["config"]["workload"],
                plot.test_of(r),
                r["config"]["variant"],
            )
            for r in selected
        },
        key=str,
    )
    if keys:
        lines.append("\n### UCall steps, first against last\n")
    for language, cores, workload, test, variant in keys:
        group = [
            r
            for r in selected
            if (
                r["config"]["language"],
                plot.cores_per_interface(r),
                r["config"]["workload"],
                plot.test_of(r),
                r["config"]["variant"],
            )
            == (language, cores, workload, test, variant)
        ]
        first, last = min(group, key=plot.step_of), max(group, key=plot.step_of)
        direction = plot.WORKLOAD_DIRECTIONS.get(workload)
        value = (
            (lambda row, direction=direction: plot.gigabits_per_card(row, direction)[0])
            if direction
            else plot.per_server_core
        )
        unit = "Gbit/s per card" if direction else "calls/s per server core"
        if first is last or value(first) is None or value(last) is None:
            continue
        lines.append(
            f"- UCall {language}, {plot.spell_cores(cores)}, {workload}, {test} test, {variant}: "
            f"step {plot.step_of(first)} "
            f"**{value(first):,.1f}** → step {plot.step_of(last)} **{value(last):,.1f}** {unit} "
            f"({value(last) / value(first):.2f}×) — {cite(first)}, {cite(last)}"
        )
    return lines


def numbers_fanout(rows: list[dict]) -> list[str]:
    selected = [
        r
        for r in measured(rows)
        if r["config"]["workload"] == "fanout" and plot.test_of(r) == "latency" and plot.plaintext_steady(r)
    ]
    lines = []
    for cores in sorted({plot.cores_per_interface(r) for r in selected}, key=str):
        lines.append(f"\n### Fan-out, {plot.spell_cores(cores)}\n")
        for row in sorted(
            (r for r in selected if plot.cores_per_interface(r) == cores),
            key=lambda r: r.get("server_cpu_microseconds_per_call") or 0,
        ):
            lines.append(
                f"- {plot.stack(row)}: **{plot.cell(row.get('server_cpu_microseconds_per_call'))} µs server CPU "
                f"per fan-out call**, {plot.per_server_core(row):,.0f} calls/s per server core, "
                f"p99 {row['latency_us']['p99']:,.0f} µs — {cite(row)}"
            )
    return lines


def numbers_security(rows: list[dict]) -> list[str]:
    selected = [r for r in measured(rows) if r["config"]["workload"] == "tool" and plot.closed_loop(r)]
    if len({r["config"]["security"] for r in selected}) < 2:
        return []
    lines = ["\n### Security modes\n"]
    for row in sorted(selected, key=lambda r: (r["config"]["phase"], plot.stack(r), r["config"]["security"])):
        value = (
            f"{plot.per_server_core(row):,.0f} handshakes/s per server core"
            if row["config"]["phase"] == "handshake"
            else f"{plot.cell(row.get('server_cpu_microseconds_per_call'))} µs server CPU per call"
        )
        lines.append(
            f"- {plot.stack(row)}, {row['config']['security']}, {row['config']['phase']}: **{value}** — {cite(row)}"
        )
    return lines


def numbers_batching(rows: list[dict]) -> list[str]:
    selected = [r for r in measured(rows) if r["config"].get("batch_size") is not None]
    lines = ["\n### Batching\n"] if selected else []
    for name in sorted({plot.stack(r) for r in selected}):
        row = best([r for r in selected if plot.stack(r) == name], plot.per_server_core)
        lines.append(
            f"- {name}: best **{plot.per_server_core(row):,.0f} calls/s per server core** at batch size "
            f"{row['config']['batch_size']}, p99 {row['latency_us']['p99']:,.0f} µs — {cite(row)}"
        )
    return lines


def numbers_load(rows: list[dict]) -> list[str]:
    selected = [r for r in measured(rows) if not plot.closed_loop(r)]
    lines = ["\n### Latency under load: the highest offered rate each stack served\n"] if selected else []
    for name in sorted({plot.stack(r) for r in selected}):
        row = best([r for r in selected if plot.stack(r) == name], lambda r: r["config"]["offered_calls_per_second"])
        latency = row["latency_us"]
        lines.append(
            f"- {name}: {row['config']['offered_calls_per_second']:,} calls/s offered "
            f"at {row['config']['connections']:,} connections, "
            f"p50 {latency['p50']:,.0f} / p99 {latency['p99']:,.0f} / p999 {latency['p999']:,.0f} µs — {cite(row)}"
        )
    return lines


def main() -> None:
    home = Path.home()
    parser = argparse.ArgumentParser(description=__doc__.partition("\n")[0])
    parser.add_argument("--run", type=Path, default=home / "run", help="The directory holding every slot's rows.")
    parser.add_argument("--article", type=Path, default=None, help="Output directory, <run>/article by default.")
    parser.add_argument(
        "--rows",
        action="append",
        default=[],
        metavar="FIGURE=DIR[,DIR]",
        help="Row directories of a figure, relative to --run, replacing its default.",
    )
    parser.add_argument("--figures", type=plot.parse_figures, default=list(plot.FIGURES), help="Figures to draw.")
    parser.add_argument(
        "--records",
        type=Path,
        action="append",
        default=None,
        help="record.sh directories, <run>/records-client and <run>/records-server by default.",
    )
    parser.add_argument(
        "--ceiling",
        type=plot.parse_ceiling,
        action="append",
        default=[],
        metavar="NAME=GBITS",
        help="A ceiling beside the iperf3 one read from <run>/ceiling, like dpdk=190.",
    )
    parser.add_argument(
        "--serveval", type=Path, default=home / "GitHub/ServEval-v1", help="The measured ServEval tree."
    )
    parser.add_argument("--ucall", type=Path, default=home / "GitHub/UCall", help="The measured UCall tree.")
    parser.add_argument("--patches", type=Path, help="The directory holding the UCall step patches STEPS names.")
    parser.add_argument("--server", help="The server box's ssh host, to hash its trees too.")
    parser.add_argument("--machine-name", help="The CPU's name in titles, the rows' CPU model if unset.")
    arguments = parser.parse_args()
    arguments.article = arguments.article or arguments.run / "article"
    arguments.records = arguments.records or [arguments.run / "records-client", arguments.run / "records-server"]
    sources = dict(FIGURE_ROWS)
    for text in arguments.rows:
        figure, _, directories = text.partition("=")
        sources[figure] = directories.split(",")
    ceiling = iperf3_ceiling(arguments.run)
    if ceiling:
        arguments.ceiling.insert(0, ("iperf3", round(ceiling[0], 1)))
    rows_directory = arguments.article / "rows"
    rows_directory.mkdir(parents=True, exist_ok=True)
    now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    index = [
        "# Article Figures and Their Sources\n",
        f"Generated {now} by `scripts/article.py`; rerun it as rows land.\n",
    ]
    index.append("## Code\n")
    index.append(f"- ServEval `{arguments.serveval}`: {tree_state(arguments.serveval, arguments.server)}")
    index.append(f"- UCall `{arguments.ucall}`: {tree_state(arguments.ucall, arguments.server)}")
    steps = arguments.run / "ucall-steps" / "STEPS"
    if steps.exists():
        patches = sorted(
            {word for line in steps.read_text().splitlines() for word in line.split() if word.endswith(".patch")}
        )
        for patch in patches:
            digest = file_digest(arguments.patches / patch) if arguments.patches else "not hashed, no --patches"
            index.append(f"- UCall step patch `{patch}`: sha256 `{digest}`, applied as `{steps}` records")
    index.append("")
    index.append("## Machines and Box Settings\n")
    for name in ("machine-client.txt", "machine-server.txt", "box-settings.md", "box-settings-server.md"):
        path = arguments.run / name
        if path.exists():
            first = path.read_text().splitlines()[0]
            modified = datetime.datetime.fromtimestamp(path.stat().st_mtime, datetime.UTC)
            index.append(f"- `{path}`: sha256 `{file_digest(path)}`, modified {modified:%Y-%m-%dT%H:%MZ}; {first}")
    for path in sorted((arguments.run / "box-tuning").glob("*.md")) if (arguments.run / "box-tuning").exists() else []:
        index.append(f"- `{path}`: sha256 `{file_digest(path)}`")
    if ceiling:
        index.append(f"- iperf3 ceiling drawn in F1: {ceiling[0]:,.1f} Gbit/s per card received, {ceiling[1]}")
    for name, gigabits in arguments.ceiling[1 if ceiling else 0 :]:
        index.append(f"- {name} ceiling: {gigabits:,.1f} Gbit/s per card, given by --ceiling")

    numbers = [
        "# Headline Numbers\n",
        (
            f"Generated {now} by `scripts/article.py`. Every number cites its row file; unsupported, void, unmerged "
            "and client-limited rows never count as framework results.\n"
        ),
    ]
    if ceiling:
        numbers.append(f"- iperf3 ceiling: **{ceiling[0]:,.1f} Gbit/s per card** received, {ceiling[1]}")
    cells = sweep_cells(arguments.run)
    if cells:
        numbers.append("\n## Network Ceiling Sweep: iperf3 Gbit/s per Card, as the Receiving End Counted\n")
        names = sorted({card for _, cards, _ in cells for card in cards})
        numbers.append("| Cell | " + " | ".join(names) + " | Directory |")
        numbers.append("| :--- | " + " | ".join("--:" for _ in names) + " | :-------- |")
        for name, cards, cell in cells:
            rates = " | ".join(f"{cards[card]:,.1f}" if card in cards else "-" for card in names)
            numbers.append(f"| {name} | {rates} | `{cell}` |")
    every_row: dict[str, list[dict]] = {}
    for figure in arguments.figures:
        directories = [arguments.run / name for name in sources[figure]]
        present = [path for path in directories if path.exists()]
        missing = [path for path in directories if not path.exists()]
        index.append(f"\n## {figure.upper()}: {FIGURE_TITLES[figure]}\n")
        if missing:
            index.append("- Not measured yet: " + ", ".join(f"`{path}`" for path in missing))
        if not present:
            continue
        svgs, rows = draw(figure, present, arguments, rows_directory)
        every_row[figure] = rows
        for svg in svgs:
            index.append(f"- Figure `{svg}`, rows and configuration in `{svg.with_suffix('.md')}`")
        if not svgs:
            index.append("- No chart: none of the rows read matches this figure")
        left_out = [row for row in rows if plot.status(row)]
        index.append(f"- Rows read: {len(rows)}, of which {len(left_out)} are not framework results")
        index.append(f"- Every row with its network figures: `{rows_directory / (figure + '.json')}`\n")
        commands: dict[str, set[str]] = {}
        for row in rows:
            commands.setdefault(str(Path(row["source"]).parent), set()).add(command(row, arguments.serveval))
        index.append("| Row directory | Rows | Client commands, one configuration each |")
        index.append("| :------------ | ---: | :-------------------------------------- |")
        for directory, lines in sorted(commands.items()):
            count = sum(1 for row in rows if str(Path(row["source"]).parent) == directory)
            logs = sorted(path.name for path in Path(directory).glob("client-*.log"))
            spelled = "<br>".join(f"`{line}`" for line in sorted(lines))
            index.append(f"| `{directory}`{' (logs: ' + ', '.join(logs) + ')' if logs else ''} | {count} | {spelled} |")

    rows_of = lambda *figures: [row for figure in figures for row in every_row.get(figure, [])]
    unique = lambda rows: list({row["source"]: row for row in rows}.values())
    sections = [
        ("Per-Core Efficiency and Scaling (F2)", numbers_scaling(unique(rows_of("f2")))),
        ("Bandwidth (F1)", numbers_bandwidth(unique(rows_of("f1")), "Bandwidth matrix")),
        ("Uploads in the Latency Matrix (F2 rows)", numbers_bandwidth(unique(rows_of("f2")), "Latency matrix")),
        ("Latency Under Load (F3)", numbers_load(unique(rows_of("f3")))),
        ("Security (F4)", numbers_security(unique(rows_of("f4")))),
        ("Batching (F5)", numbers_batching(unique(rows_of("f5")))),
        ("Fan-Out Through Each Server's Client (F6)", numbers_fanout(unique(rows_of("f6")))),
        ("Connection Density (F7)", numbers_memory(unique(rows_of("f7")))),
        ("UCall Steps (F9)", numbers_steps(unique(rows_of("f9")))),
        (
            "Python (F10)",
            numbers_scaling([row for row in unique(rows_of("f10")) if row["config"]["language"] == "python"]),
        ),
    ]
    for title, lines in sections:
        numbers.append(f"\n## {title}")
        numbers += lines or ["\nNo measured rows yet."]
    arguments.article.mkdir(parents=True, exist_ok=True)
    (arguments.article / "INDEX.md").write_text("\n".join(index) + "\n")
    (arguments.article / "NUMBERS.md").write_text("\n".join(numbers) + "\n")
    print(arguments.article / "INDEX.md")
    print(arguments.article / "NUMBERS.md")


if __name__ == "__main__":
    main()
