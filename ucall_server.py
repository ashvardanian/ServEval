"""UCall's Python binding serving the tool, context and fan-out workloads, and batched tool calls.

One server answers on every interface, its event loops shared by all of them, each polled by its own thread running an
asyncio loop for its streamed uploads beside its bound fan-out client. Noise IK or XX secures it. It runs on
free-threaded CPython, so the loops serve in parallel.
"""

import asyncio
import collections
import json
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
STOPPING = {signal.SIGINT, signal.SIGTERM}


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
interfaces = [
    ucall.Interface(
        host_port(interface.addresses[0], args.port),
        device_name=interface.device,
        security=security,
        noise_private_key=server_key,
        peer_authorizer=(lambda key: key == client_public) if noise else None,
        event_loops=args.threads,
        connections=arguments.admission(args),
    )
    for interface in args.interfaces
]
server = ucall.Server(*interfaces)
if server.event_loops != args.threads:
    arguments.exit(f"UCall runs {server.event_loops} event loops, not {args.threads}")
# Each loop shares the upstream connections among the downstream calls it may hold.
UPSTREAM_CONNECTIONS = -(-arguments.admission(args) // args.threads) * max(len(upstreams), 1)


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
    """One event loop's bound client, its fan-outs waiting to be admitted, and its uploads in flight."""

    def __init__(self, event_loop: int) -> None:
        self.client = ucall.Client(
            connections=UPSTREAM_CONNECTIONS,
            noise_private_key=client_key,
            peer_authorizer=(lambda key: key == server_public) if noise else None,
            server=server,
            event_loop=event_loop,
        )
        self.waiting: collections.deque[Fanout] = collections.deque()
        self.uploads: set[asyncio.Task] = set()
        self.pulls = 0

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
            fanout.unsent -= 1
            if not fanout.unsent:
                self.waiting.popleft()
                self.settle(fanout)

    def forward(self) -> None:
        """Hands the waiting calls to the client, and answers the fan-outs whose replies all arrived."""
        self.submit()
        while finished := self.client.take_completed(capacity=UPSTREAM_CONNECTIONS):
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


def unit(request: ucall.Request) -> None:
    reply_json(request, stats.unit_json())


def health(request: ucall.Request) -> None:
    reply_json(request, payloads.health("ucall", "ucall", True))


def batch(requests: list[ucall.Request]) -> None:
    for request in requests:
        tool(request)


def drain(batches: ucall.BatchEndpoint) -> None:
    signal.pthread_sigmask(signal.SIG_BLOCK, STOPPING)
    batches.take_batches()


async def serve(event_loop: int) -> None:
    """Polls `event_loop` until the server stops, between polls forwarding fan-outs and letting uploads pull."""
    signal.pthread_sigmask(signal.SIG_BLOCK, STOPPING)
    loop = loops[threading.get_ident()] = Loop(event_loop)
    wait = POLL_SLICE_MICROSECONDS
    while server.take_call(event_loop, wait):
        loop.forward()
        started = loop.pulls
        await asyncio.sleep(0)
        # A pull the loop answers from bytes it already holds finishes inside a poll without waking it, so the poll
        # after new pulls does not wait.
        wait = 0 if loop.pulls != started else UPLOAD_SLICE_MICROSECONDS if loop.uploads else POLL_SLICE_MICROSECONDS


if __name__ == "__main__":
    server.add_endpoint("/raw", "POST", raw)
    server.add_endpoint("/tool", "POST", tool)
    server.add_endpoint("/tool/{name}", "POST", full)
    server.add_endpoint("/context", "PUT", context, body=ucall.BodyDelivery.STREAMED)
    server.add_endpoint("/fanout", "POST", fanout)
    server.add_endpoint("/stats", "GET", unit)
    server.add_endpoint("/health", "GET", health)
    wait = round(args.batch_wait * 1e6)
    batches = server.add_batch_endpoint(
        "/tool/batch", "POST", batch, min_size=args.batch_size, max_size=args.batch_size, max_latency_microseconds=wait
    )
    for stopping in STOPPING:
        signal.signal(stopping, lambda *_: server.stop())
    pollers = [threading.Thread(target=asyncio.run, args=(serve(index),)) for index in range(server.event_loops)]
    pollers.append(threading.Thread(target=drain, args=(batches,)))
    for poller in pollers:
        poller.start()
    print(f"Listening on {', '.join(server.addresses)} with {server.event_loops} UCall event loops", flush=True)
    for poller in pollers:
        poller.join()
