"""UCall's Python binding serving the tool, context, download and fan-out workloads, and batched tool calls.

One server answers on every interface, its event loops shared by all of them, each polled by its own thread running an
asyncio loop for its streamed uploads beside its bound fan-out client. Noise IK or XX secures it. It runs on
free-threaded CPython, so the loops serve in parallel.

UCall starts no threads, so this program places its own. Python has no ForkUnion binding, so each polling thread pins
itself to one core of the affinity mask local to the memory domain UCall reports for its interface, before its first
poll. The loops number those cores, whatever `--threads` says. Those polling threads are the only ones serving: between
polls each claims every `/tool/batch` endpoint without waiting and answers whatever batch is due.
"""

import asyncio
import collections
import itertools
import json
import os
import pathlib
import signal
import threading

import ucall

import arguments
import payloads
import stats

args = arguments.server_parser(__doc__.partition("\n")[0]).parse_args()
if args.security == "tls":
    arguments.exit("UCall secures connections with Noise; use --security none, noise-xx or noise-ik")
SECURITY = {"none": ucall.Security.NONE, "noise-xx": ucall.Security.NOISE_XX, "noise-ik": ucall.Security.NOISE_IK}
JSON = ("content-type: application/json",)
OCTETS = ("content-type: application/octet-stream",)
TEXT = ("content-type: text/plain",)
UPSTREAM_HEADERS = ("content-type: application/json", "accept-encoding: identity")
# The longest one poll waits, as the binding's own `Server.run` slices it, and while uploads pull, the longest pause
# the binding's `_until_final` takes between two looks at a pull, so the pull is seen once it finished.
POLL_SLICE_MICROSECONDS, UPLOAD_SLICE_MICROSECONDS = 100_000, 1_000
BATCH_WAIT_MICROSECONDS = round(args.batch_wait * 1e6)
STOPPING = {signal.SIGINT, signal.SIGTERM}
# Requests one connection may have in flight: RFC 9113's recommended minimum of concurrent streams, the depth tonic and
# grpc.aio allow when they advertise no limit, so every stack's throughput test runs at one depth.
MAX_PIPELINED_REQUESTS = arguments.RFC_9113_MINIMUM_CONCURRENT_STREAMS


def read_key(name: str) -> bytes:
    """The key keys.sh wrote as 64 hexadecimal digits to `<data-dir>/noise/<name>`."""
    path = pathlib.Path(args.data_dir) / "noise" / name
    try:
        key = bytes.fromhex(path.read_text().strip())
    except (OSError, ValueError):
        key = b""
    if len(key) != 32:
        arguments.exit(f"{path} holds no Noise key; run keys.sh with UCall's ucall tool on PATH")
    return key


def host_port(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


security = SECURITY[args.security]
noise = security != ucall.Security.NONE
server_key, server_public = (read_key("server.key"), read_key("server.pub")) if noise else (None, None)
client_key, client_public = (read_key("client.key"), read_key("client.pub")) if noise else (None, None)
scheme = "http+noise" if noise else "http"
upstreams = [f"{scheme}://{host_port(host, port)}/tool" for host, port in args.upstreams or []]


def probe_memory_domains() -> tuple[int, ...]:
    """The memory domain UCall reports for each interface's device, from a throwaway server on port 0: a server fixes
    its loops at construction, and the loops follow from the domains."""
    probes = [
        ucall.Interface(
            host_port(interface.addresses[0], 0), device_name=interface.device, event_loops=1, connections=1
        )
        for interface in args.interfaces
    ]
    return ucall.Server(*probes).memory_domains


def cores_near(memory_domain: int) -> list[int]:
    """The cores of `memory_domain` this process may run on, from the kernel's node list and the unit's mask."""
    node = stats.parse_cpus(pathlib.Path(f"/sys/devices/system/node/node{memory_domain}/cpulist").read_text())
    return sorted(set(node) & set(ALLOWED_CPUS))


def placements(memory_domains: tuple[int, ...]) -> dict[int, list[int]]:
    """The cores of each memory domain hosting an interface, in the order the interfaces name them; interfaces on one
    domain share its cores. Refuses an interface with no domain or no allowed core near it, since polling it from
    another domain is the cost this placement exists to avoid."""
    near: dict[int, list[int]] = {}
    for interface, memory_domain in zip(args.interfaces, memory_domains):
        if memory_domain < 0:
            arguments.exit(f"UCall reports no memory domain for {interface}; name its device as DEVICE=ADDRESS")
        if memory_domain in near:
            continue
        near[memory_domain] = cores_near(memory_domain)
        if not near[memory_domain]:
            arguments.exit(
                f"no core of the mask {stats.spell_cpus(ALLOWED_CPUS)} is local to memory domain {memory_domain} of "
                f"{interface}"
            )
    return near


# The main thread's mask is the unit's, as systemd's `AllowedCPUs` sets it; only the polling threads narrow theirs.
ALLOWED_CPUS = stats.allowed_cpus()
MEMORY_DOMAINS = probe_memory_domains()
CORES_NEAR = placements(MEMORY_DOMAINS)
EVENT_LOOPS = sum(len(cores) for cores in CORES_NEAR.values())
# Each memory domain's first loop: its loops follow those of the domains before it.
FIRST_LOOPS = dict(zip(CORES_NEAR, itertools.accumulate(map(len, CORES_NEAR.values()), initial=0)))
interfaces = [
    ucall.Interface(
        host_port(interface.addresses[0], args.port),
        device_name=interface.device,
        security=security,
        noise_private_key=server_key,
        peer_authorizer=(lambda key: key == client_public) if noise else None,
        event_loops=EVENT_LOOPS,
        connections=arguments.admission(args),
        pipelined_requests=MAX_PIPELINED_REQUESTS,
    )
    for interface in args.interfaces
]
server = ucall.Server(*interfaces)
if server.event_loops != EVENT_LOOPS:
    arguments.exit(f"UCall runs {server.event_loops} event loops, not {EVENT_LOOPS}")
# Each loop shares the upstream connections among the downstream calls it may hold. Without upstreams no loop makes a
# client, whose budget would otherwise grow every loop's queue by connections it never opens.
UPSTREAM_CONNECTIONS = -(-arguments.admission(args) // EVENT_LOOPS) * len(upstreams)


def check(call: dict) -> int:
    """`after + top_k` of a tool call, refusing values that are not integers."""
    after, top_k = call["arguments"]["filters"]["after"], call["arguments"]["top_k"]
    if not (type(after) is int and type(top_k) is int and type(call["id"]) is str):
        raise ValueError("a tool call types its id, after and top_k")
    return after + top_k


class Fanout:
    """A fan-out its polling thread forwards to the upstreams and answers once every reply arrived."""

    def __init__(self, request: ucall.Request, body: bytes, id: str, check: int, width: int) -> None:
        self.request, self.body, self.width = request, body, width
        self.expected = payloads.tool_reply(id, check)
        self.reply = payloads.fanout_reply(id, width)
        self.unsent = self.unfinished = width
        self.failed = False


class Loop:
    """One event loop's bound client, none without upstreams, its fan-outs waiting to be admitted, and its uploads in
    flight."""

    def __init__(self, event_loop: int) -> None:
        self.client = (
            ucall.Client(
                connections=UPSTREAM_CONNECTIONS,
                noise_private_key=client_key,
                peer_authorizer=(lambda key: key == server_public) if noise else None,
                server=server,
                event_loop=event_loop,
            )
            if upstreams
            else None
        )
        self.waiting: collections.deque[Fanout] = collections.deque()
        self.uploads: set[asyncio.Task] = set()
        # Download replies still sending the prepared body in place, kept so none is released unfinished here, on the
        # one thread that finishes it.
        self.downloads: collections.deque[ucall.Deferred] = collections.deque()
        self.pulls = 0
        # Upstream calls the client admitted and has not completed, so the poll knows when no fan-out needs forwarding.
        self.calls_in_flight = 0

    def settle(self, fanout: Fanout) -> None:
        if fanout.unfinished:
            return
        if fanout.failed:
            fanout.request.reply_error(502, "")
        else:
            fanout.request.reply(fanout.reply, headers=JSON)

    def submit(self) -> None:
        """Admits the waiting calls in order until the client is full."""
        while self.waiting:
            fanout = self.waiting[0]
            url = upstreams[(fanout.width - fanout.unsent) % len(upstreams)]
            try:
                self.client.submit("POST", url, fanout.body, UPSTREAM_HEADERS, security=security,
                                   noise_peer_key=server_public, context=fanout)  # fmt: skip
            except ucall.Error as error:
                if error.status == ucall.Status.CONTENDED:
                    return
                fanout.unfinished -= 1
                fanout.failed = True
            else:
                self.calls_in_flight += 1
            fanout.unsent -= 1
            if not fanout.unsent:
                self.waiting.popleft()
                self.settle(fanout)

    def forward(self) -> None:
        """Hands the waiting calls to the client, and answers the fan-outs whose replies all arrived."""
        if self.client is None:
            return  # without upstreams no fan-out waits
        self.submit()
        while finished := self.client.take_completed(capacity=UPSTREAM_CONNECTIONS):
            self.calls_in_flight -= len(finished)
            for completed in finished:
                fanout, response = completed.context, completed.response
                expected = response is not None and response.status_code == 200 and response.body == fanout.expected
                fanout.failed |= not expected
                fanout.unfinished -= 1
                self.settle(fanout)
            self.submit()


loops: dict[int, Loop] = {}


def reply_json(request: ucall.Request, body: bytes) -> None:
    request.reply(body, headers=JSON)


def raw(request: ucall.Request) -> None:
    request.reply(bytes(request.body[: payloads.REPLY_BYTES]), headers=OCTETS)


def tool(request: ucall.Request) -> None:
    try:
        call = json.loads(bytes(request.body))
        reply = payloads.tool_reply(call["id"], check(call))
    except (ValueError, KeyError, TypeError):
        return request.reply_error(400, "")
    reply_json(request, reply)


def full(request: ucall.Request) -> None:
    if request.header("authorization") != payloads.AUTHORIZATION:
        return request.reply_error(401, "")
    trace = payloads.trace_id(request.header("traceparent"))
    try:
        call = json.loads(bytes(request.body))
        name, lang, after = request.path_parameter("name"), request.query("lang"), int(request.query("after"))
        if None in (trace, name, lang) or type(call["top_k"]) is not int or type(call["id"]) is not str:
            raise ValueError("a full call names its tool, trace, lang, id and top_k")
        reply = payloads.full_reply(call["id"], name, trace, lang, after + call["top_k"])
    except (ValueError, KeyError, TypeError):
        return request.reply_error(400, "")
    reply_json(request, reply)


async def upload(request: ucall.DetachedRequest, loop: Loop) -> None:
    """Counts a streamed body as its chunks arrive, then answers with the count."""
    count, chunks = 0, request.body_chunks()
    try:
        while True:
            loop.pulls += 1
            count += len(await anext(chunks))
    except StopAsyncIteration:
        request.reply(payloads.count_reply(count), headers=TEXT)
    except ucall.Error:
        request.reply_error(400, "")


def context(request: ucall.Request) -> None:
    loop = loops[threading.get_ident()]
    task = asyncio.get_running_loop().create_task(upload(request.detach(), loop))
    loop.uploads.add(task)
    task.add_done_callback(loop.uploads.discard)


def download(request: ucall.Request) -> None:
    """Replies with the first `bytes` bytes of the body prepared on the first download, which UCall sends in place as a
    memory part, without copying it; 416 when the server prepared fewer, 500 when it could not prepare any."""
    prepared = arguments.download_body(args)
    if prepared is None:
        return request.reply_error(500, "")
    try:
        count = int(request.query("bytes"))
    except (TypeError, ValueError):
        return request.reply_error(400, "")
    body = payloads.download(prepared, count)
    if body is None:
        return request.reply_error(416, "")
    loop = loops[threading.get_ident()]
    # A loop finishes its replies roughly in order, so the oldest final ones leave in amortized constant time.
    while loop.downloads and loop.downloads[0].status() != ucall.Status.DEFERRED:
        loop.downloads.popleft()
    sent = request.detach().reply_parts([ucall.Memory(body)], headers=OCTETS)
    if sent is not None:
        loop.downloads.append(sent)


def fanout(request: ucall.Request) -> None:
    body = bytes(request.body)
    try:
        call = json.loads(body)
        width = int(request.query("width"))
        if width < 1:
            raise ValueError("a fan-out calls at least one upstream")
        prepared = Fanout(request, body, call["id"], check(call), width)
    except (ValueError, KeyError, TypeError):
        return request.reply_error(400, "")
    if not upstreams:
        return request.reply_error(503, "")
    loop = loops[threading.get_ident()]
    loop.waiting.append(prepared)


CPU_LIST = stats.spell_cpus(ALLOWED_CPUS)


def unit(request: ucall.Request) -> None:
    # A polling thread's own mask is its one core, so the reply names the process's mask, read before any pinning.
    reply = stats.unit_stats() | {"cores": len(ALLOWED_CPUS), "cpu_list": CPU_LIST}
    # UCall's own counters, summed over the loops, so rows price kernel enters, completions and sends per request.
    counters = [server.event_loop_statistics(event_loop) for event_loop in range(server.event_loops)]
    reply["event_loop_counters"] = stats.summed_counters(counters)
    reply_json(request, json.dumps(reply, separators=(",", ":")).encode())


HEALTH = payloads.health(
    "ucall", "ucall", args.batch_size, max_pipelined_requests_per_connection=MAX_PIPELINED_REQUESTS
)


def health(request: ucall.Request) -> None:
    reply_json(request, HEALTH)


def batch(requests: list[ucall.Request]) -> None:
    for request in requests:
        tool(request)


def take_due_batches() -> None:
    """Answers each batch endpoint's due batch unless another polling thread holds that endpoint now.

    Every polling thread claims the endpoints in turn, so a batch fires on the first one to look once it is due: on the
    thread whose deposit filled it, or for a partial batch on the first poll to end after its oldest request waited
    `--batch-wait`. Polls wait at most `--batch-wait`, so that one ends within `--batch-wait` of the deadline.
    """
    for endpoint in batches:
        try:
            endpoint.take_batch()  # a zero timeout never sleeps: it claims, fires what is due, and releases
        except ucall.Error as error:
            if error.status != ucall.Status.CONTENDED:
                raise


async def serve(event_loop: int) -> None:
    """Polls `event_loop` until the server stops, between polls forwarding fan-outs and letting uploads pull."""
    signal.pthread_sigmask(signal.SIG_BLOCK, STOPPING)
    loop = loops[threading.get_ident()] = Loop(event_loop)
    poll_slice = min(POLL_SLICE_MICROSECONDS, BATCH_WAIT_MICROSECONDS)
    upload_slice = min(UPLOAD_SLICE_MICROSECONDS, BATCH_WAIT_MICROSECONDS)
    wait, take = poll_slice, server.take_call
    while take(event_loop, wait):
        take_due_batches()
        if not (loop.waiting or loop.calls_in_flight or loop.uploads):
            wait = poll_slice
            continue  # Steady tool calls stay in C: one call per poll, no asyncio step.
        loop.forward()
        started = loop.pulls
        await asyncio.sleep(0)
        # A pull the loop answers from bytes it already holds finishes inside a poll without waking it, so the poll
        # after new pulls does not wait.
        wait = 0 if loop.pulls != started else upload_slice if loop.uploads else poll_slice


def poll_pinned(core: int, event_loop: int) -> None:
    os.sched_setaffinity(0, [core])  # pins this thread before its first poll touches the loop's memory
    asyncio.run(serve(event_loop))


if __name__ == "__main__":
    # Tool calls answer PUT as well as POST: UCall's client pipelines only idempotent calls, so deeper clients PUT.
    for verb in ("POST", "PUT"):
        server.add_endpoint("/raw", verb, raw)
        server.add_endpoint("/tool", verb, tool)
        server.add_endpoint("/tool/{name}", verb, full)
    server.add_endpoint("/context", "PUT", context, body=ucall.BodyDelivery.STREAMED)
    server.add_endpoint("/context", "GET", download)
    server.add_endpoint("/fanout", "POST", fanout)
    server.add_endpoint("/stats", "GET", unit)
    server.add_endpoint("/health", "GET", health)
    batches = [
        server.add_batch_endpoint(
            "/tool/batch",
            verb,
            batch,
            min_size=args.batch_size,
            max_size=args.batch_size,
            max_latency_microseconds=BATCH_WAIT_MICROSECONDS,
        )
        for verb in ("POST", "PUT")
    ]
    for stopping in STOPPING:
        signal.signal(stopping, lambda *_: server.stop())
    pollers = [
        threading.Thread(target=poll_pinned, args=(core, FIRST_LOOPS[memory_domain] + index))
        for memory_domain, cores in CORES_NEAR.items()
        for index, core in enumerate(cores)
    ]
    for poller in pollers:
        poller.start()
    for interface, address, memory_domain in zip(args.interfaces, server.addresses, MEMORY_DOMAINS):
        first, cores = FIRST_LOOPS[memory_domain], CORES_NEAR[memory_domain]
        print(f"Interface {interface} at {address}: memory domain {memory_domain}, cores {stats.spell_cpus(cores)}, "
              f"event loops {first}-{first + len(cores) - 1} of {EVENT_LOOPS}")  # fmt: skip
    print(f"Listening on {', '.join(server.addresses)} with {server.event_loops} UCall event loops", flush=True)
    for poller in pollers:
        poller.join()
