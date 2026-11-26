"""Flags shared by every Python server and client, each defaulting to its `SERVEVAL_<FLAG>` variable."""

import argparse
import ctypes
import dataclasses
import ipaddress
import os
import pathlib
import socket
import sys
import time
from typing import NoReturn

import payloads


def exit(message: str) -> NoReturn:
    """Prints `message` and exits with 1."""
    print(message, file=sys.stderr)
    sys.exit(1)


def add_flag(parser: argparse.ArgumentParser, flag: str, default, **options) -> None:
    """Adds `flag`, defaulting to its `SERVEVAL_*` variable, the name the Rust pairs read."""
    variable = "SERVEVAL_" + flag.removeprefix("--").replace("-", "_").upper()
    parse = options.pop("type", str)

    def checked(text: str):
        try:
            return parse(text)
        except ValueError as error:
            exit(f'{flag}="{text}" does not parse, {error}')

    # A string default parses through `type`, as the command line would.
    help = f"{options.pop('help')} [env: {variable}]"
    parser.add_argument(flag, default=os.environ.get(variable, default), type=checked, help=help, **options)


def parse_count(text: str) -> int:
    """A count like `10000` or `10M`: `K`, `M`, `G`, `T` are powers of 1000 in any case, and zero is refused."""
    digits = text.rstrip("kKmMgGtT")
    suffix = text[len(digits) :].lower()
    if not (digits.isascii() and digits.isdigit()) or suffix not in ("", "k", "m", "g", "t") or int(digits) == 0:
        raise ValueError("expected a positive count like 1000 or 10M")
    return int(digits) * 1000 ** ("", "k", "m", "g", "t").index(suffix)


def parse_count_or_zero(text: str) -> int:
    """A count like `parse_count`, or `0`."""
    return 0 if text == "0" else parse_count(text)


def parse_size(text: str) -> int:
    """A size like `4096`, `64KB` or `16GB`, in powers of 1024; zero is refused."""
    upper = text.upper()
    for level, unit in reversed(list(enumerate(("", "KB", "MB", "GB", "TB")))):
        digits = upper.removesuffix(unit) if unit else upper
        if (unit == "" or upper.endswith(unit)) and digits.isascii() and digits.isdigit() and int(digits) != 0:
            return int(digits) << (10 * level)
    raise ValueError("expected a size like 4096, 64KB or 16GB")


def spell_size(count: int) -> str:
    """Spells a size the way `parse_size` reads it: `16GB`, `1000`."""
    unit = ""
    for larger in ("KB", "MB", "GB", "TB"):
        if count == 0 or count % 1024:
            break
        count, unit = count // 1024, larger
    return f"{count}{unit}"


def parse_duration(text: str) -> float:
    """A duration like `200ms` or `10s`, in seconds; a bare number, a fraction or zero is refused."""
    for unit, scale in (("ms", 1e-3), ("s", 1.0)):
        digits = text.removesuffix(unit)
        if text.endswith(unit) and digits.isascii() and digits.isdigit() and int(digits) != 0:
            return int(digits) * scale
    raise ValueError("expected a duration like 200ms or 10s")


def spell_duration(seconds: float) -> str:
    """Spells a duration the way `parse_duration` reads it: `1s`, `1500ms`."""
    milliseconds = round(seconds * 1000)
    return f"{milliseconds // 1000}s" if milliseconds % 1000 == 0 else f"{milliseconds}ms"


def parse_seed(text: str) -> int:
    """A 32-bit unsigned integer, or `random` as 32 bits from the OS entropy source."""
    if text == "random":
        return int.from_bytes(os.urandom(4), "little")
    if not (text.isascii() and text.isdigit()) or int(text) >= 1 << 32:
        raise ValueError("expected an unsigned integer or random")
    return int(text)


def all_cores() -> int:
    """Every core in this process's affinity mask; macOS has no mask, so every core there."""
    if sys.platform == "darwin":
        return os.cpu_count() or 1
    return len(os.sched_getaffinity(0))


def parse_threads(text: str) -> int:
    """A thread count like `8`, or `0` as every core in the affinity mask."""
    if text == "0":
        return all_cores()
    try:
        return parse_count(text)
    except ValueError:
        raise ValueError("expected a count, 0 for all cores") from None


def parse_port(text: str) -> int:
    """A TCP port from 1 to 65535."""
    if not (text.isascii() and text.isdigit()) or not 0 < int(text) < 65536:
        raise ValueError("expected a port from 1 to 65535")
    return int(text)


@dataclasses.dataclass(frozen=True)
class Interface:
    """A local device and its addresses; an empty device routes by address alone."""

    device: str | None
    addresses: tuple[str, ...]

    def __str__(self) -> str:
        addresses = "+".join(self.addresses)
        return f"{self.device}={addresses}" if self.device else addresses


def parse_interface(text: str) -> Interface:
    """`DEVICE=ADDRESS[+ADDRESS...]` or `ADDRESS[+ADDRESS...]`."""
    device, _, addresses = text.rpartition("=")
    try:
        parsed = tuple(str(ipaddress.ip_address(address)) for address in addresses.split("+"))
    except ValueError:
        parsed = ()
    if not parsed or ("=" in text and not device):
        raise ValueError("expected DEVICE=ADDRESS or ADDRESS, extra addresses joined by +")
    return Interface(device or None, parsed)


def parse_address(text: str) -> str:
    try:
        return str(ipaddress.ip_address(text))
    except ValueError:
        raise ValueError("expected an IP address") from None


def parse_upstream(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    if not host or not (port.isascii() and port.isdigit()):
        raise ValueError("expected ADDRESS:PORT")
    return parse_address(host.strip("[]")), parse_port(port)


# The `--calls-in-flight-per-connection` that reads the server's `max_concurrent_streams_per_connection` from `/health`.
SERVER_MAXIMUM = "server-maximum"
# RFC 9113 section 5.1.2 recommends that a server's SETTINGS_MAX_CONCURRENT_STREAMS "be no smaller than 100", so a
# server advertising no limit gets the depth every conforming server allows.
RFC_9113_MINIMUM_CONCURRENT_STREAMS = 100
# Why a client that keeps one call in flight per connection cannot run a deeper configuration.
ONE_CALL_PER_CONNECTION = "client keeps one call in flight per connection"
# Why a configuration the server answers 404, 405 or 501, or gRPC's UNIMPLEMENTED, cannot run.
NOT_SERVED = "server does not serve this call"


def parse_calls_in_flight(text: str) -> int | str:
    """A positive count of calls each connection keeps in flight, or `server-maximum`."""
    if text == SERVER_MAXIMUM:
        return text
    if not (text.isascii() and text.isdigit()) or int(text) == 0:
        raise ValueError(f"expected positive integers or {SERVER_MAXIMUM}")
    return int(text)


def list_of(parse):
    """A comma-separated list of `parse` values."""
    return lambda text: [parse(item) for item in text.split(",")]


def one_of(*choices: str):
    """A comma-separated list of `choices`."""

    def parse(text: str) -> list[str]:
        values = text.split(",")
        if not all(value in choices for value in values):
            raise ValueError(f"expected one of {', '.join(choices)}")
        return values

    return parse


def choice(*choices: str):
    """One of `choices`."""

    def parse(text: str) -> str:
        if text not in choices:
            raise ValueError(f"expected one of {', '.join(choices)}")
        return text

    return parse


def _common(parser: argparse.ArgumentParser) -> None:
    add_flag(parser, "--threads", "0", type=parse_threads, help="Workers, 0 for every core in the affinity mask.")
    add_flag(
        parser,
        "--interfaces",
        "127.0.0.1",
        type=list_of(parse_interface),
        help="Local interfaces as DEVICE=ADDRESS or ADDRESS, extra addresses joined by +.",
    )
    add_flag(parser, "--port", "8545", type=parse_port, help="Port every server listens on.")
    add_flag(
        parser, "--security", "none", type=choice("none", "noise-xx", "noise-ik", "tls"), help="Transport security."
    )
    add_flag(parser, "--data-dir", "data", help="Directory holding the tls and noise keys that keys.sh writes.")
    add_flag(parser, "--seed", "42", type=parse_seed, help="Seed of every payload, or random to draw one.")


def _body_bytes(parser: argparse.ArgumentParser) -> None:
    add_flag(
        parser,
        "--body-bytes",
        None,
        type=list_of(parse_size),
        help="Context upload and download sizes to sweep; unset, powers of 4 from one TCP segment's payload at the "
        "interfaces' MTU up to net.ipv4.tcp_rmem's maximum.",
    )


# RFC 791's IPv4 header without options, RFC 8200's fixed IPv6 header, and RFC 9293's TCP header without options.
IPV4_HEADER_BYTES, IPV6_HEADER_BYTES, TCP_HEADER_BYTES = 20, 40, 20
# The one upload size of the runs before the sweep, kept in the default sweep so new rows compare with theirs.
EARLIER_UPLOAD_BYTES = 256 << 10
# RFC 7323's timestamps option, 10 bytes padded to 12, which Linux puts on every segment while
# `net.ipv4.tcp_timestamps` is on.
TCP_TIMESTAMPS_OPTION_BYTES = 12


class _InterfaceAddresses(ctypes.Structure):
    """One entry of the list `getifaddrs` returns, `struct ifaddrs` in <ifaddrs.h>."""


_InterfaceAddresses._fields_ = [
    ("next", ctypes.POINTER(_InterfaceAddresses)),
    ("name", ctypes.c_char_p),
    ("flags", ctypes.c_uint),
    ("address", ctypes.c_void_p),
    ("netmask", ctypes.c_void_p),
    ("broadcast_or_destination", ctypes.c_void_p),
    ("data", ctypes.c_void_p),
]


def _address_of(socket_address: int | None):
    """The IP address a `struct sockaddr` holds, None for other families or no address. `struct sockaddr_in` keeps
    the IPv4 address at byte 4, `struct sockaddr_in6` the IPv6 one at byte 8."""
    family = ctypes.c_ushort.from_address(socket_address).value if socket_address else None
    start, length = {socket.AF_INET: (4, 4), socket.AF_INET6: (8, 16)}.get(family, (0, 0))
    return ipaddress.ip_address(ctypes.string_at(socket_address + start, length)) if length else None


def interface_addresses() -> list[tuple[str, ipaddress.IPv4Address | ipaddress.IPv6Address]]:
    """Every device and address in the kernel's interface address list."""
    library = ctypes.CDLL(None, use_errno=True)
    listed = ctypes.POINTER(_InterfaceAddresses)()
    if library.getifaddrs(ctypes.byref(listed)) != 0:
        return []
    found, entry = [], listed
    try:
        while entry:
            address = _address_of(entry.contents.address)
            if address is not None:
                found.append((entry.contents.name.decode(), address))
            entry = entry.contents.next
        return found
    finally:
        library.freeifaddrs(listed)


def device_holding(address: str) -> str | None:
    """The name of the device holding `address`, from the kernel's interface address list."""
    wanted = ipaddress.ip_address(address)
    return next((device for device, held in interface_addresses() if held == wanted), None)


def devices_of(address: str) -> list[str]:
    """The devices an address names: the one holding it, or every device holding an address for a wildcard, which
    listens on all of them."""
    wanted = ipaddress.ip_address(address)
    listed = interface_addresses()
    return sorted({device for device, held in listed if wanted.is_unspecified or held == wanted})


def segment_payload(interface: Interface) -> int:
    """The smallest MTU among the interface's devices less its IP header and the TCP header, the MSS before options."""
    address = interface.addresses[0]
    devices = [interface.device] if interface.device else devices_of(address)
    if not devices:
        raise ValueError(f"no device holds {address}, so its MTU is unknown")
    mtu = min(int(pathlib.Path(f"/sys/class/net/{device}/mtu").read_text()) for device in devices)
    header = IPV6_HEADER_BYTES if ":" in address else IPV4_HEADER_BYTES
    return max(mtu - header - TCP_HEADER_BYTES, 0)


def default_body_bytes(interfaces: list[Interface]) -> list[int]:
    """Powers of 4 times the payload one TCP segment carries, from that payload up to `net.ipv4.tcp_rmem`'s maximum,
    the most one socket's receive buffer grows to, with `EARLIER_UPLOAD_BYTES` among them. The payload is the smallest
    MTU of the interfaces' devices, from `/sys/class/net/<device>/mtu`, less the IP and TCP headers and the timestamps
    option when it is on. An interface given as an address alone names the device holding that address, and a wildcard
    address every device."""
    payload = min(segment_payload(interface) for interface in interfaces)
    if pathlib.Path("/proc/sys/net/ipv4/tcp_timestamps").read_text().strip() != "0":
        payload = max(payload - TCP_TIMESTAMPS_OPTION_BYTES, 0)
    largest = int(pathlib.Path("/proc/sys/net/ipv4/tcp_rmem").read_text().split()[2])
    if payload == 0:
        raise ValueError("the interfaces' MTU leaves no TCP payload")
    sizes = [payload]
    while sizes[-1] * 4 <= largest:
        sizes.append(sizes[-1] * 4)
    return sorted({*sizes, EARLIER_UPLOAD_BYTES})


def body_bytes(args: argparse.Namespace) -> list[int]:
    """The `--body-bytes` sizes, or `default_body_bytes` of the interfaces, derived on first use; exits when they
    cannot be."""
    if args.body_bytes is None:
        try:
            args.body_bytes = default_body_bytes(args.interfaces)
        except (OSError, ValueError, IndexError) as error:
            exit(f"{error}; pass --body-bytes")
    return args.body_bytes


_prepared: dict[str, bytes | None] = {}


def download_body(args: argparse.Namespace) -> bytes | None:
    """The bytes every download is a prefix of, for the largest `--body-bytes` of this server's interfaces, prepared
    on the first download, so a server that never serves one never derives the sweep; None, said once on stderr,
    when the sweep is neither given nor derivable."""
    if "body" not in _prepared:
        try:
            sizes = args.body_bytes or default_body_bytes(args.interfaces)
        except (OSError, ValueError, IndexError) as error:
            print(f"downloads: {error}", file=sys.stderr)
            sizes = None
        _prepared["body"] = payloads.context_body(args.seed, max(sizes)) if sizes else None
    return _prepared["body"]


def server_parser(description: str) -> argparse.ArgumentParser:
    """The flags every Python server takes."""
    parser = argparse.ArgumentParser(description=description)
    _common(parser)
    add_flag(
        parser,
        "--connections",
        "10K",
        type=list_of(parse_count),
        help="Connection counts; the largest is the admission budget and listen backlog.",
    )
    add_flag(parser, "--upstreams", None, type=list_of(parse_upstream), help="Fan-out targets as ADDRESS:PORT.")
    add_flag(parser, "--batch-size", "32", type=parse_count, help="Most calls one batch gathers.")
    add_flag(
        parser,
        "--batch-wait",
        "10ms",
        type=parse_duration,
        help="Longest a partial batch's oldest call waits before it fires. The UCall servers fire it on the next poll "
        "after that, at most one poll slice later: the smaller of this wait and POLL_SLICE, 100 ms.",
    )
    _body_bytes(parser)
    return parser


def client_parser(description: str) -> argparse.ArgumentParser:
    """The flags every Python client takes."""
    parser = argparse.ArgumentParser(description=description)
    _common(parser)
    add_flag(
        parser,
        "--targets",
        "127.0.0.1",
        type=list_of(parse_address),
        help="Server addresses, paired with --interfaces by position.",
    )
    add_flag(parser, "--connections", "64", type=list_of(parse_count), help="Connection counts to sweep.")
    add_flag(
        parser,
        "--workloads",
        "tool,context,download,fanout",
        type=one_of("tool", "context", "download", "fanout"),
        help="Workloads.",
    )
    add_flag(parser, "--variants", "plain,batched", type=one_of("plain", "batched"), help="Variants.")
    add_flag(parser, "--phases", "steady,handshake", type=one_of("steady", "handshake"), help="Connection phases.")
    add_flag(
        parser,
        "--parsing",
        "body",
        type=one_of("raw", "body", "full"),
        help="Tool call shapes: raw bytes, a typed body, or a typed body with path, query and header values.",
    )
    _body_bytes(parser)
    add_flag(
        parser,
        "--calls-in-flight-per-connection",
        "1",
        type=list_of(parse_calls_in_flight),
        help=f"Calls each connection keeps in flight to sweep, or {SERVER_MAXIMUM} for the server's stream limit.",
    )
    add_flag(
        parser, "--calls-per-second", None, type=parse_count, help="Scheduled calls per second; unset is closed-loop."
    )
    add_flag(parser, "--width", "8", type=parse_count, help="Upstream calls per fan-out.")
    add_flag(parser, "--time-limit", "10s", type=parse_duration, help="Measured time per configuration.")
    add_flag(parser, "--warmup", "1s", type=parse_duration, help="Unmeasured calls before each measured window.")
    add_flag(parser, "--memory-limit", None, type=parse_size, help="Client memory budget, at most the cgroup's.")
    add_flag(
        parser,
        "--between-runs",
        "reconnect",
        type=choice("keep", "reconnect"),
        help="Reuse the open connections, or close them and open fresh ones for each configuration.",
    )
    add_flag(parser, "--output", "results", help="Directory for <framework>-<config-hash>.json reports.")
    return parser


# Idle connections stay open across a whole sweep, as the Rust servers keep them, instead of Uvicorn's 5s.
KEEP_ALIVE_SECONDS = 86_400
# How long after the last client finished connecting the window opens.
BARRIER_DELAY_SECONDS = 0.1


def admission(args: argparse.Namespace) -> int:
    """The server's admission budget: the largest of the swept connection counts."""
    return max(args.connections)


def require_no_device(args: argparse.Namespace, framework: str) -> None:
    """Exits if any interface names a device, for frameworks that bind by address alone."""
    if any(interface.device for interface in args.interfaces):
        exit(f"{framework} binds by address alone; list interfaces as ADDRESS")


def single_interface(args: argparse.Namespace, framework: str) -> Interface:
    """The one interface a competitor process binds; launch.py starts one process per memory domain."""
    if len(args.interfaces) != 1:
        exit(f"{framework} binds one interface per process; launch.py starts one per memory domain")
    return args.interfaces[0]


@dataclasses.dataclass(frozen=True)
class Run:
    """One configuration; `parsing` is swept only for plain tool calls, `body` for every other run. `in_flight` is
    `connections` times `calls_in_flight_per_connection`, whose `_source` is `flag`, `server-advertised` or
    `rfc9113-minimum`, and `unsupported` names why the client cannot run it, None when it can."""

    workload: str
    variant: str
    parsing: str
    phase: str
    connections: int
    calls_in_flight_per_connection: int
    calls_in_flight_per_connection_source: str
    in_flight: int
    body_bytes: int
    unsupported: str | None

    @property
    def test(self) -> str:
        """`throughput` when calls are amortized, by more than one in flight per connection or by server-side batching,
        and `latency` otherwise."""
        amortized = self.calls_in_flight_per_connection > 1 or self.variant == "batched"
        return "throughput" if amortized else "latency"


# Why a batched configuration has no measurement: the server ships no batching, or uploads and downloads stream one
# body per call.
SERVER_DOES_NOT_BATCH = "server does not batch calls"
UPLOADS_NOT_BATCHED = "uploads stream one body per call, so they are not batched"
DOWNLOADS_NOT_BATCHED = "downloads stream one body per call, so they are not batched"


def calls_in_flight_per_connection(requested: int | str, advertised: int | None) -> tuple[int, str]:
    """The depth one connection carries and where the number came from: the flag, the server's advertised
    `max_concurrent_streams_per_connection`, or RFC 9113's recommended minimum when it advertises no limit."""
    if requested != SERVER_MAXIMUM:
        return requested, "flag"
    if advertised is None:
        return RFC_9113_MINIMUM_CONCURRENT_STREAMS, "rfc9113-minimum"
    if advertised == 0:
        exit("the server advertises 0 concurrent streams per connection, so no call can run")
    return advertised, "server-advertised"


def runs(args: argparse.Namespace, batching: bool, multiplexed: bool, advertised: int | None) -> list[Run]:
    """Every configuration; batched variants only where the server batches, and never for uploads or downloads. A
    client that does not multiplex keeps one call in flight per connection, so its deeper configurations are
    `unsupported`."""
    depths = [
        calls_in_flight_per_connection(requested, advertised) for requested in args.calls_in_flight_per_connection
    ]
    result = []
    for workload in args.workloads:
        sizes = body_bytes(args) if workload in ("context", "download") else [0]
        for variant in args.variants:
            shapes = args.parsing if (workload, variant) == ("tool", "plain") else ["body"]
            for parsing in shapes:
                for phase in args.phases:
                    for connections in args.connections:
                        for depth, source in depths:
                            unsupported = None if multiplexed or depth == 1 else ONE_CALL_PER_CONNECTION
                            if variant == "batched" and workload == "context":
                                unsupported = UPLOADS_NOT_BATCHED
                            elif variant == "batched" and workload == "download":
                                unsupported = DOWNLOADS_NOT_BATCHED
                            elif variant == "batched" and not batching:
                                unsupported = SERVER_DOES_NOT_BATCH
                            in_flight = connections * depth
                            result += [
                                Run(
                                    workload,
                                    variant,
                                    parsing,
                                    phase,
                                    connections,
                                    depth,
                                    source,
                                    in_flight,
                                    size,
                                    unsupported,
                                )
                                for size in sizes
                            ]
    return result


def share(total: int, index: int, parts: int) -> int:
    """Part `index` of `total` split evenly over `parts`, the first parts taking the remainder."""
    return total // parts + (index < total % parts)


def interface_of(args: argparse.Namespace, index: int) -> Interface:
    """The local interface paired with target `index`, a single interface serving every target."""
    return args.interfaces[index % len(args.interfaces)]


def listen(interface: Interface, port: int, backlog: int) -> socket.socket:
    """A listener on the interface's first address, bound to its device, with `backlog` pending connections."""
    address = interface.addresses[0]
    listener = socket.socket(socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Accepted connections inherit it, so no framework waits on Nagle's algorithm.
    listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if interface.device:
        if not hasattr(socket, "SO_BINDTODEVICE"):
            exit(f"binding device {interface.device} needs Linux")
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, interface.device.encode())
    listener.bind((address, port))
    listener.listen(backlog)
    return listener


def process() -> tuple[int, int]:
    """This process's place among the clients launch.py starts, from `SERVEVAL_PROCESS` as `INDEX/COUNT`."""
    text = os.environ.get("SERVEVAL_PROCESS", "")
    if not text:
        return 0, 1
    index, _, count = text.partition("/")
    if not (index.isascii() and index.isdigit() and count.isascii() and count.isdigit() and int(index) < int(count)):
        exit(f'SERVEVAL_PROCESS="{text}" does not parse, expected INDEX/COUNT like 0/2')
    return int(index), int(count)


def start_at(run: int) -> float:
    """When run `run` starts on `time.monotonic`: 100ms after the last launched client finished connecting for it.
    Each client writes its Unix nanoseconds to `<SERVEVAL_BARRIER>/<run>-<process>` and waits for the files of every
    process, or starts now without the variable."""
    directory = os.environ.get("SERVEVAL_BARRIER")
    if directory is None:
        return time.monotonic()
    index, count = process()
    mine = pathlib.Path(directory) / f"{run}-{index}"
    staged = mine.with_suffix(".tmp")
    try:
        staged.write_text(str(time.time_ns()))
        staged.replace(mine)
    except OSError as error:
        exit(f'SERVEVAL_BARRIER="{directory}" is not writable: {error}')
    paths = [pathlib.Path(directory) / f"{run}-{other}" for other in range(count)]
    while not all(path.exists() for path in paths):
        time.sleep(0.001)
    latest = max(int(path.read_text()) for path in paths)
    return time.monotonic() + latest / 1e9 + BARRIER_DELAY_SECONDS - time.time()
