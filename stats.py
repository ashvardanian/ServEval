"""Latency histograms that merge exactly across processes, the call schedule, and cgroup and process counters."""

import asyncio
import dataclasses
import json
import math
import pathlib
import resource
import time

import arguments

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


def unit_stats() -> dict:
    """This process's cgroup v2 counters, covering every worker process, None where there is no cgroup v2, and the
    box's `tcp_active_opens`."""
    stats = {
        "cpu_microseconds": None,
        "memory_bytes": None,
        "cores": arguments.all_cores(),
        "memory_limit": None,
        "tcp_active_opens": tcp_active_opens(),
    }
    directory = _cgroup_directory()
    if directory is None:
        return stats
    limits = [_read_integer(ancestor / "memory.max") for ancestor in (directory, *directory.parents)]
    stats["cpu_microseconds"] = _read_integer(directory / "cpu.stat", "usage_usec ")
    stats["memory_bytes"] = _read_integer(directory / "memory.current")
    stats["memory_limit"] = min((limit for limit in limits if limit is not None), default=None)
    return stats


def unit_json() -> bytes:
    """The `/stats` body."""
    return json.dumps(unit_stats(), separators=(",", ":")).encode()


def process_cpu() -> float:
    """User and system CPU seconds this process and its waited-for children have used."""
    own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime
