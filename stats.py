"""Latency histograms that merge exactly across processes, the call schedule, and cgroup and process counters."""

import asyncio
import dataclasses
import json
import math
import os
import pathlib
import resource
import threading
import time

SUB_BINS = 16
EXACT_BITS = 5
BINS = (64 - EXACT_BITS + 2) * SUB_BINS
# Why a call failed, as `stats.rs` names it: `connect` is any transport failure but a refusal or a timeout.
FAILURES = ("connect", "refused", "timeout", "status", "wrong_reply")
# Pause between two samples of the box's sockets and a server's memory, and the most samples one reconnect waits for.
SETTLE_INTERVAL_SECONDS = 0.1
SETTLE_ROUNDS = 100


def bin_of(nanoseconds: int) -> int:
    """The bin of a latency, as `stats.rs` and UCall's driver place it."""
    if nanoseconds < 2 * SUB_BINS:
        return nanoseconds
    shift = nanoseconds.bit_length() - EXACT_BITS
    return shift * SUB_BINS + (nanoseconds >> shift)


def upper_edge(bin: int) -> int:
    if bin < 2 * SUB_BINS:
        return bin
    shift = bin // SUB_BINS - 1
    return ((bin % SUB_BINS + SUB_BINS + 1) << shift) - 1


def percentile(bins: list[int], fraction: float) -> int:
    """The upper edge in nanoseconds of the bin holding the nearest-rank `fraction`."""
    total = sum(bins)
    if not total:
        return 0
    rank, cumulative = max(1, math.ceil(fraction * total)), 0
    for bin, count in enumerate(bins):
        cumulative += count
        if cumulative >= rank:
            return upper_edge(bin)
    return upper_edge(BINS - 1)


def sparse(bins: list[int]) -> list[list[int]]:
    """The non-empty bins as `[bin, count]` pairs."""
    return [[bin, count] for bin, count in enumerate(bins) if count]


def dense(pairs: list[list[int]]) -> list[int]:
    bins = [0] * BINS
    for bin, count in pairs:
        bins[bin] += count
    return bins


def failure_of(error: BaseException | None) -> str:
    """The class of a transport error by the first refusal or timeout among its causes, `connect` without one."""
    while error is not None:
        if isinstance(error, ConnectionRefusedError):
            return "refused"
        if isinstance(error, TimeoutError):
            return "timeout"
        error = error.__cause__ or error.__context__
    return "connect"


@dataclasses.dataclass
class Tally:
    """What a set of calls returned; `all_calls` includes the warmup, which the server's CPU time covers, and `unsent`
    counts the measured calls the schedule owed that the window closed on before they left."""

    bins: list[int] = dataclasses.field(default_factory=lambda: [0] * BINS)
    lateness: list[int] = dataclasses.field(default_factory=lambda: [0] * BINS)
    calls: int = 0
    failures: dict[str, int] = dataclasses.field(default_factory=lambda: dict.fromkeys(FAILURES, 0))
    unsent: int = 0
    all_calls: int = 0
    request_body_bytes: int = 0
    response_body_bytes: int = 0

    def merge(self, other: "Tally") -> None:
        self.bins = [mine + theirs for mine, theirs in zip(self.bins, other.bins)]
        self.lateness = [mine + theirs for mine, theirs in zip(self.lateness, other.lateness)]
        self.calls += other.calls
        self.failures = {name: count + other.failures[name] for name, count in self.failures.items()}
        self.unsent += other.unsent
        self.all_calls += other.all_calls
        self.request_body_bytes += other.request_body_bytes
        self.response_body_bytes += other.response_body_bytes


@dataclasses.dataclass(frozen=True)
class Window:
    """A warmup, then the measured window, closed-loop or on one global arrival schedule, on `time.monotonic`."""

    begin: float
    measured_from: float
    until: float
    calls_per_second: int | None
    slots: int

    @classmethod
    def starting(cls, begin: float, warmup: float, time_limit: float, calls_per_second: int | None, slots: int):
        return cls(begin, begin + warmup, begin + warmup + time_limit, calls_per_second, slots)

    @property
    def seconds(self) -> float:
        return self.until - self.measured_from

    def unix_ms(self) -> list[int]:
        """The measured window's first and last moment in Unix milliseconds, to find it in other recordings."""
        offset = time.time() - time.monotonic()
        return [int((moment + offset) * 1000) for moment in (self.measured_from, self.until)]


def _record(bins: list[int], seconds: float) -> None:
    bins[min(bin_of(max(int(seconds * 1e9), 0)), BINS - 1)] += 1


async def slot_loop(window: Window, slot: int, call) -> Tally:
    """Issues calls from one slot until the window ends; `await call(sequence)` returns the body bytes sent and
    received, or one of `FAILURES`. Open-loop latency counts from the intended send, so a stalled server cannot hide
    its queue, and the calls a late slot never sent count as unsent, so a slow client cannot pass for a fast server."""
    tally = Tally()
    await asyncio.sleep(max(window.begin - time.monotonic(), 0))
    arrival, rate = 0, window.calls_per_second
    while True:
        sequence = slot + arrival * window.slots
        arrival += 1
        intended = window.begin + sequence / rate if rate else time.monotonic()
        if intended >= window.until or time.monotonic() >= window.until:
            owed = range(sequence, int(rate * (window.until - window.begin)) + 1, window.slots) if rate else ()
            tally.unsent += sum(window.measured_from <= window.begin + late / rate < window.until for late in owed)
            return tally
        if rate:
            await asyncio.sleep(max(intended - time.monotonic(), 0))
        if intended >= window.measured_from:
            _record(tally.lateness, time.monotonic() - intended)
        outcome = await call(sequence)
        latency = time.monotonic() - intended
        tally.all_calls += 1
        if intended < window.measured_from:
            continue
        if isinstance(outcome, str):
            tally.failures[outcome] += 1
            continue
        tally.calls += 1
        tally.request_body_bytes += outcome[0]
        tally.response_body_bytes += outcome[1]
        _record(tally.bins, latency)


def _cgroup_directory() -> pathlib.Path | None:
    try:
        membership = pathlib.Path("/proc/self/cgroup").read_text()
    except OSError:
        return None
    for line in membership.splitlines():
        if line.startswith("0::"):
            return pathlib.Path("/sys/fs/cgroup") / line[3:].lstrip("/")
    return None


def _read_integer(path: pathlib.Path, prefix: str = "") -> int | None:
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        value = line.removeprefix(prefix)
        if line.startswith(prefix) and value.strip().isascii() and value.strip().isdigit():
            return int(value)
    return None


def tcp_active_opens() -> int | None:
    """TCP connections this box opened since boot, every process together, from `/proc/net/snmp`."""
    try:
        tcp = [
            line.split()[1:]
            for line in pathlib.Path("/proc/net/snmp").read_text().splitlines()
            if line.startswith("Tcp:")
        ]
        return int(tcp[1][tcp[0].index("ActiveOpens")])
    except (OSError, IndexError, ValueError):
        return None


def sockets_in_use() -> list[int] | None:
    """The box's TCP sockets in use and orphaned, from `/proc/net/sockstat`."""
    try:
        lines = pathlib.Path("/proc/net/sockstat").read_text().splitlines()
        fields = next(line.split()[1:] for line in lines if line.startswith("TCP:"))
        return [int(fields[fields.index(name) + 1]) for name in ("inuse", "orphan")]
    except (OSError, StopIteration, IndexError, ValueError):
        return None


def settled(earlier: tuple, later: tuple) -> bool:
    """Whether nothing moved between two samples of the box's sockets and a server's memory: the same sockets, and
    memory within 1%."""
    (sockets, memory), (later_sockets, later_memory) = earlier, later
    if None in (memory, later_memory):
        return sockets == later_sockets and memory == later_memory
    return sockets == later_sockets and abs(later_memory - memory) * 100 <= memory


def parse_cpus(text: str) -> list[int]:
    """The cores of a cpuset list like `8-9,104-105`, in order; a malformed range is skipped."""
    cpus = []
    for part in filter(None, text.strip().split(",")):
        first, _, last = part.partition("-")
        last = last or first
        if first.isascii() and first.isdigit() and last.isascii() and last.isdigit():
            cpus += range(int(first), int(last) + 1)
    return cpus


def spell_cpus(cpus) -> str:
    """Spells cores as a cpuset list like `8-9,104-105`, the way `parse_cpus` reads it."""
    ranges, ordered = [], sorted(set(cpus))
    for index, cpu in enumerate(ordered):
        if index == 0 or ordered[index - 1] != cpu - 1:
            ranges.append([cpu, cpu])
        else:
            ranges[-1][1] = cpu
    return ",".join(f"{first}-{last}" if last != first else f"{first}" for first, last in ranges)


def allowed_cpus() -> list[int]:
    """The cores this process may run on: its `sched_getaffinity` mask, or every core where the platform has none."""
    if not hasattr(os, "sched_getaffinity"):
        return list(range(os.cpu_count() or 1))
    return sorted(os.sched_getaffinity(0))


def unit_name() -> str | None:
    """This process's systemd unit, the last component of its cgroup v2 path like
    `serveval-server-axum-8545-0.service`, or None outside a cgroup v2 hierarchy or at its root."""
    directory = _cgroup_directory()
    return None if directory is None or directory == pathlib.Path("/sys/fs/cgroup") else directory.name


class MemoryPeak:
    """The cgroup's `memory.peak` since the previous read, through one descriptor this process holds open.

    Writing to an open `memory.peak` resets the peak only that descriptor reports, to the current usage, which Linux
    allows since 6.12; so a client's `/stats` before and after its window bracket that window's peak. The descriptor
    opens on the first read and again on every read until it opens: `memory.peak` belongs to root unless the launcher
    hands it to the server's user, and a peak that cannot be reset would cover the unit's whole life, so it reads as
    None instead. A process with several workers holds one descriptor per worker, and a reply covers the time since
    the answering worker's previous `/stats`."""

    def __init__(self) -> None:
        self.descriptor: int | None = None
        # Two `/stats` replies served at once must not interleave one's read with the other's reset.
        self.lock = threading.Lock()

    def take(self) -> int | None:
        """The peak since the previous `take`, or since the cgroup started for the first, then resets it."""
        with self.lock:
            directory = _cgroup_directory()
            if self.descriptor is None and directory is not None:
                try:
                    self.descriptor = os.open(directory / "memory.peak", os.O_RDWR | os.O_CLOEXEC)
                except OSError:
                    return None
            if self.descriptor is None:
                return None
            try:
                peak = int(os.pread(self.descriptor, 32, 0))
                # The kernel takes any non-empty write as a reset and ignores the offset.
                os.pwrite(self.descriptor, b"reset\n", 0)
            except (OSError, ValueError):
                return None
            return peak


memory_peak = MemoryPeak()


def unit_stats() -> dict:
    """The `/stats` reply: this process's cgroup v2 counters, which cover every process of its systemd unit, with the
    unit's cores and the box's `tcp_active_opens`. Each server runs in its own unit, so a reply covers one server.

    - `cpu_microseconds`: the cgroup's `usage_usec` from `cpu.stat`.
    - `memory_current_bytes`: the cgroup's `memory.current`.
    - `memory_peak_bytes`: the cgroup's `memory.peak` since the previous `/stats`, as `MemoryPeak` reads it.
    - `memory_limit`: the smallest `memory.max` of the cgroup and its ancestors.
    - `cores` and `cpu_list`: the `sched_getaffinity` mask, counted and spelled like `8-9`.
    - `unit`: the cgroup's last path component, like `serveval-server-axum-8545-0.service`.
    - `tcp_active_opens`: TCP connections the box opened since boot, every process together.

    A counter is None where its file is missing, as outside a cgroup v2 hierarchy."""
    cpus = allowed_cpus()
    stats = {
        "cpu_microseconds": None,
        "memory_current_bytes": None,
        "memory_peak_bytes": memory_peak.take(),
        "memory_limit": None,
        "cores": len(cpus),
        "cpu_list": spell_cpus(cpus),
        "unit": unit_name(),
        "tcp_active_opens": tcp_active_opens(),
    }
    directory = _cgroup_directory()
    if directory is None:
        return stats
    limits = [_read_integer(ancestor / "memory.max") for ancestor in (directory, *directory.parents)]
    stats["cpu_microseconds"] = _read_integer(directory / "cpu.stat", "usage_usec ")
    stats["memory_current_bytes"] = _read_integer(directory / "memory.current")
    stats["memory_limit"] = min((limit for limit in limits if limit is not None), default=None)
    return stats


def unit_json() -> bytes:
    """The `/stats` body."""
    return json.dumps(unit_stats(), separators=(",", ":")).encode()


# The `/stats` fields `serveval.proto`'s `StatsReply` has no field for, which the gRPC pairs send as response metadata,
# each under its name with dashes for underscores.
METADATA_FIELDS = ("memory_peak_bytes", "cpu_list", "unit")


def grpc_reply(stats: dict) -> tuple[dict, tuple[tuple[str, str], ...]]:
    """The gRPC `Stats` reply of `unit_stats()`: the `StatsReply` fields, whose `memory_bytes` carries
    `memory_current_bytes`, and the metadata holding the fields it lacks."""
    fields = {"memory_bytes": stats["memory_current_bytes"]}
    fields |= {key: stats[key] for key in ("cpu_microseconds", "cores", "memory_limit", "tcp_active_opens")}
    metadata = tuple((key.replace("_", "-"), str(stats[key])) for key in METADATA_FIELDS if stats[key] is not None)
    return {key: value for key, value in fields.items() if value is not None}, metadata


def from_grpc_reply(reply, metadata) -> dict:
    """Reads a reply `grpc_reply` made; a field missing from it reads as None."""
    entries = dict(metadata or ())

    def field(name: str):
        return getattr(reply, name) if reply.HasField(name) else None

    peak = entries.get("memory-peak-bytes", "")
    return {
        "cpu_microseconds": field("cpu_microseconds"),
        "memory_current_bytes": field("memory_bytes"),
        "memory_peak_bytes": int(peak) if peak.isascii() and peak.isdigit() else None,
        "memory_limit": field("memory_limit"),
        "cores": reply.cores,
        "cpu_list": entries.get("cpu-list", ""),
        "unit": entries.get("unit"),
        "tcp_active_opens": field("tcp_active_opens"),
    }


def server_stats(replies: list[dict]) -> dict:
    """The `/stats` replies of every target as one server's: counters summed, `cpu_list` unioned, and `units` listing
    each unit once. A unit serving several targets, like UCall's one server on both network interfaces, answers each
    alike, so only its first reply counts: a second would count its cores twice and see its memory peak just reset.
    A counter missing from any counted reply is None, and `tcp_active_opens`, the box's, is the largest."""
    counted, units = [], []
    for reply in replies:
        unit = reply.get("unit")
        if unit is not None and unit in units:
            continue
        counted.append(reply)
        if unit is not None:
            units.append(unit)

    def total(key: str) -> int | None:
        values = [reply.get(key) for reply in counted]
        return None if None in values else sum(values)

    opens = [reply.get("tcp_active_opens") for reply in replies]
    return {
        "cpu_microseconds": total("cpu_microseconds"),
        "memory_current_bytes": total("memory_current_bytes"),
        "memory_peak_bytes": total("memory_peak_bytes"),
        "memory_limit": total("memory_limit"),
        "cores": sum(reply.get("cores") or 0 for reply in counted),
        "cpu_list": spell_cpus(cpu for reply in counted for cpu in parse_cpus(reply.get("cpu_list") or "")),
        "units": units,
        "tcp_active_opens": None if None in opens else max(opens, default=None),
        "event_loop_counters": summed_counters([reply.get("event_loop_counters") for reply in counted]),
    }


def summed_counters(all_counters: list[dict | None]) -> dict | None:
    """Event-loop counters summed by name, None unless every entry keeps them."""
    if not all_counters or None in all_counters:
        return None
    sums: dict[str, int] = {}
    for counters in all_counters:
        for name, value in counters.items():
            sums[name] = sums.get(name, 0) + value
    return sums


def counter_growth(before: dict | None, after: dict | None) -> dict | None:
    """Each event-loop counter's growth from `before` to `after`, one server's counters sampled around a window."""
    if before is None or after is None:
        return None
    return {name: max(value - before.get(name, 0), 0) for name, value in after.items()}


def per_request(growth: dict | None) -> dict | None:
    """Each nonzero counter of `growth` per request the loops parsed over the same span; None without requests."""
    requests = (growth or {}).get("requests", 0)
    if not requests:
        return None
    return {name: value / requests for name, value in growth.items() if value}


def process_cpu() -> float:
    """User and system CPU seconds this process and its waited-for children have used."""
    own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime
