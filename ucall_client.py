"""UCall's Python client driving a UCall server, one client per thread on free-threaded CPython.

The threads live across configurations, so their clients can stay open under `--between-runs keep`; each opens its
share of the connections up front by concurrent health checks, then drives its slots on the schedule `stats.slot_loop`
keeps, waiting on the network exactly until the next call is due. UCall's client binds neither a device nor an address,
so the kernel routes each target by its subnet.

Each thread unpickles its own copy of the calls, their options and the window. On free-threaded CPython an object
another thread made takes atomic reference counts, and a dictionary another thread reads takes its lock, so objects
every thread touched on every call would cap the client, not the server.

UCall pipelines only idempotent calls, per RFC 9112 section 9.3.2, so a run keeping several calls in flight per
connection sends its tool calls as PUT, which the UCall servers answer as they answer POST; context uploads already are.
A fan-out's POST is not idempotent, so its deeper runs, and handshake runs, whose calls close their connection, are
`unsupported`.
"""

import dataclasses
import heapq
import json
import pathlib
import pickle
import queue
import threading
import time

import ucall

import arguments
import payloads
import results
import stats

args = arguments.client_parser(__doc__.partition("\n")[0]).parse_args()
if args.security == "tls":
    arguments.exit("UCall secures connections with Noise; use --security none, noise-xx or noise-ik")
SECURITY = {"none": ucall.Security.NONE, "noise-xx": ucall.Security.NOISE_XX, "noise-ik": ucall.Security.NOISE_IK}
HEADERS = ("content-type: application/json", "accept-encoding: identity")
IDENTITY, CLOSE = "accept-encoding: identity", "connection: close"
UNSUPPORTED = (404, 405, 501)
# UCall's default call budget, which bounds every wait for a completion.
CALL_TIMEOUT_MICROSECONDS = 30_000_000
# Why a fan-out run keeping several calls in flight per connection cannot run.
FANOUT_NOT_IDEMPOTENT = "fan-out calls are not idempotent"
# Why a handshake run keeping several calls in flight per connection cannot run: UCall pipelines no call behind one
# that closes its connection.
HANDSHAKE_CLOSES = "handshake calls close their connection"


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


security = SECURITY[args.security]
noise = security != ucall.Security.NONE
client_key, server_public = (read_key("client.key"), read_key("server.pub")) if noise else (None, None)
scheme = "http+noise" if noise else "http"
origins = [
    f"{scheme}://[{target}]:{args.port}" if ":" in target else f"{scheme}://{target}:{args.port}"
    for target in args.targets
]
ring = payloads.tool_ring(args.seed)
# The body each upload sends and each download must get back, per size.
bodies = {"context", "download"} & set(args.workloads)
contexts = {size: payloads.context_body(args.seed, size) for size in arguments.body_bytes(args)} if bodies else {}
options = {"security": security, "noise_peer_key": server_public}


def open_client(connections: int, depth: int = 1, calls: int = 0) -> ucall.Client:
    """A client of `connections` connections, each pipelining up to `depth` of the `calls` it accepts at once,
    admitting the servers' key under Noise XX."""
    authorize = (lambda key: key == server_public) if noise else None
    return ucall.Client(
        connections=connections,
        pipelined_calls=depth,
        calls=calls,
        noise_private_key=client_key,
        peer_authorizer=authorize,
        max_response_bytes=max_response_bytes,
    )


class Exchange:
    """One call as every thread sends it to one target, and the reply it must get back byte for byte."""

    def __init__(self, verb: str, url: str, body: bytes, headers: tuple[str, ...], reply: bytes) -> None:
        self.verb, self.url, self.body, self.headers, self.reply = verb, url, body, headers, reply


def exchanges_of(run: arguments.Run) -> list[list[Exchange]]:
    """Per target, one exchange per ring entry, or the one context upload or download."""
    closing = (CLOSE,) if run.phase == "handshake" else ()
    if run.workload == "context":
        body = contexts[run.body_bytes]
        reply = payloads.count_reply(len(body))
        return [[Exchange("PUT", f"{origin}/context", body, HEADERS + closing, reply)] for origin in origins]
    if run.workload == "download":
        path, reply = payloads.download_path(run.body_bytes), contexts[run.body_bytes]
        return [[Exchange("GET", origin + path, b"", HEADERS + closing, reply)] for origin in origins]
    calls = payloads.exchanges(ring, run, args.width)
    verb = "POST" if run.calls_in_flight_per_connection == 1 else "PUT"  # only idempotent calls pipeline

    def headers_of(call: payloads.Exchange) -> tuple[str, ...]:
        traced = (f"authorization: {payloads.AUTHORIZATION}", f"traceparent: {call.traceparent}")
        return HEADERS + (traced if call.traceparent else ()) + closing

    return [
        [Exchange(verb, origin + call.path, call.body, headers_of(call), call.reply) for call in calls]
        for origin in origins
    ]


def get(control: ucall.Client, target: int, path: str) -> ucall.Response:
    return control.get(origins[target] + path, headers=(IDENTITY, CLOSE), **options)


def server_stats(control: ucall.Client) -> dict:
    """The servers' `/stats` replies as one server's, as `stats.server_stats` sums them."""
    # A server too busy to answer its control call within the timeout leaves this sample empty, as the httpx client's
    # does, instead of ending the client while its workers still run.
    replies = []
    for target in range(len(origins)):
        try:
            replies.append(json.loads(bytes(get(control, target, "/stats").body)))
        except ucall.Error:
            replies.append({})
    return stats.server_stats(replies)


def record(bins: list[int], seconds: float) -> None:
    bins[min(stats.bin_of(max(int(seconds * 1e9), 0)), stats.BINS - 1)] += 1


# RFC 9112's status line of a 200 reply, with RFC 9110's reason phrase, as UCall's servers write it.
STATUS_LINE_OK = "HTTP/1.1 200 OK\r\n"
# The largest reply a call buffers when downloads are swept, set once from the servers' own reply; 0 is UCall's default.
max_response_bytes = 0


def largest_reply_bytes(control: ucall.Client, largest: int) -> int | None:
    """The largest download and the head of the servers' reply to it, as RFC 9112 frames it: the status line, every
    `name: value` field line and the empty line. One streamed download of that size is pulled and dropped, so every
    buffered download fits; None when the server does not answer it with 200. It sends a steady call's headers, whose
    reply's `connection: keep-alive` is the longer of the two forms."""
    url = origins[0] + payloads.download_path(largest)
    response = control.get(url, headers=HEADERS, body_delivery=ucall.BodyDelivery.STREAMED, **options)
    fields = sum(len(name) + len(b": ") + len(value) + len(b"\r\n") for name, value in response.fields())
    for _ in response.body_chunks():
        pass  # its connection carries nothing else until the body is pulled
    return largest + len(STATUS_LINE_OK) + fields + len("\r\n") if response.status_code == 200 else None


def outcome_of(completed: ucall.Completed, exchange: Exchange) -> tuple[int, int] | str:
    """The body bytes sent and received by a finished call, or one of `stats.FAILURES`."""
    response = completed.response
    if response is None:
        return "timeout" if completed.status == ucall.Status.TIMEOUT else "connect"
    if response.status_code != 200:
        return "status"
    return (len(exchange.body), len(exchange.reply)) if response.body == exchange.reply else "wrong_reply"


def measure(
    client: ucall.Client, exchanges: list[list[Exchange]], options: dict, window: stats.Window, index: int, threads: int
):
    """Runs slots `index + i * threads` of the window, slot `i` on target `i % targets`, each with one call
    outstanding, as `stats.slot_loop` schedules them."""
    tally, rate = stats.Tally(), window.calls_per_second
    slots = range(index, window.slots, threads)
    arrivals, flights, due = [0] * len(slots), [(0.0, 0)] * len(slots), []

    def exchange_of(position: int, sequence: int) -> Exchange:
        calls = exchanges[slots[position] // threads % len(exchanges)]
        return calls[sequence % len(calls)]

    def queue_next(position: int) -> None:
        sequence = slots[position] + arrivals[position] * window.slots
        arrivals[position] += 1
        intended = window.begin + sequence / rate if rate else time.monotonic()
        if intended < window.until and time.monotonic() < window.until:
            heapq.heappush(due, (intended, position, sequence))
            return
        owed = range(sequence, int(rate * (window.until - window.begin)) + 1, window.slots) if rate else ()
        tally.unsent += sum(window.measured_from <= window.begin + late / rate < window.until for late in owed)

    def finish(position: int, outcome: tuple[int, int] | str) -> None:
        intended = flights[position][0]
        tally.all_calls += 1
        if intended >= window.measured_from and isinstance(outcome, str):
            tally.failures[outcome] += 1
        elif intended >= window.measured_from:
            tally.calls += 1
            tally.request_body_bytes += outcome[0]
            tally.response_body_bytes += outcome[1]
            record(tally.bins, time.monotonic() - intended)
        queue_next(position)

    time.sleep(max(window.begin - time.monotonic(), 0))
    for position in range(len(slots)):
        queue_next(position)
    outstanding = 0
    while outstanding or due:
        now = time.monotonic()
        while due and due[0][0] <= now:
            intended, position, sequence = heapq.heappop(due)
            if intended >= window.measured_from:
                record(tally.lateness, now - intended)
            flights[position] = (intended, sequence)
            exchange = exchange_of(position, sequence)
            try:
                client.submit(exchange.verb, exchange.url, exchange.body, exchange.headers, context=position,
                              **options)  # fmt: skip
                outstanding += 1
            except ucall.Error as error:
                finish(position, "timeout" if error.status == ucall.Status.TIMEOUT else "connect")
        wait = max(due[0][0] - now, 0) if due else None
        if not outstanding:
            time.sleep(wait or 0)
            continue
        timeout = CALL_TIMEOUT_MICROSECONDS if wait is None else max(round(wait * 1e6), 1)
        for completed in client.take_completed(timeout_microseconds=timeout, capacity=len(slots)):
            position = completed.context
            outstanding -= 1
            finish(position, outcome_of(completed, exchange_of(position, flights[position][1])))
    return tally


def work(index: int, threads: int, inbox: queue.SimpleQueue, outbox: queue.SimpleQueue, sampled) -> None:
    """One thread's client across configurations: it opens its share of the connections, reports ready, runs the
    window it then receives, reports its tally, and keeps its connections open until the servers were sampled."""
    kept, limit = None, None
    while (load := inbox.get()) is not None:
        run, exchanges, options = pickle.loads(load)
        connections = max(arguments.share(run.connections, index, threads), 1)
        depth = run.calls_in_flight_per_connection
        # The slots this thread drives, which may exceed its connections' depth by the remainders of the two splits.
        budget = (connections, depth, max(arguments.share(run.in_flight, index, threads), connections * depth))
        if kept is None or limit != budget:
            kept = None
            kept, limit = open_client(*budget), budget
        if run.phase == "steady":
            for target, origin in enumerate(origins):
                for _ in range(arguments.share(connections, target, len(origins))):
                    kept.submit("GET", origin + "/health", headers=(IDENTITY,), **options)
            answered = 0
            while answered < connections:
                answered += len(kept.take_completed(CALL_TIMEOUT_MICROSECONDS, connections))
        outbox.put(None)
        window = pickle.loads(inbox.get())
        outbox.put(measure(kept, exchanges, options, window, index, threads))
        sampled.wait()
        if args.between_runs == "reconnect":
            kept = None


if __name__ == "__main__":
    control = open_client(len(origins))
    health = json.loads(bytes(get(control, 0, "/health").body))
    # The row names this client, not the one `/health` names for the server's own pair.
    report = results.Report(args, health["framework"], "ucall", health.get("batch_size"))
    if "download" in args.workloads:
        max_response_bytes = largest_reply_bytes(control, max(arguments.body_bytes(args))) or 0
        control = open_client(len(origins))
    threads = args.threads
    sampled = threading.Barrier(threads + 1)
    inboxes, outbox = [queue.SimpleQueue() for _ in range(threads)], queue.SimpleQueue()
    workers = [
        threading.Thread(target=work, args=(index, threads, inbox, outbox, sampled))
        for index, inbox in enumerate(inboxes)
    ]
    for worker in workers:
        worker.start()
    # `server-maximum` is the server's pipelining depth, or, from a server that reports none, its stream limit.
    advertised = health.get("max_pipelined_requests_per_connection")
    if advertised is None:
        advertised = health.get("max_concurrent_streams_per_connection")
    for position, run in enumerate(arguments.runs(args, health["batching"], True, advertised)):
        if run.calls_in_flight_per_connection > 1 and run.workload == "fanout":
            run = dataclasses.replace(run, unsupported=FANOUT_NOT_IDEMPOTENT)
        elif run.calls_in_flight_per_connection > 1 and run.phase == "handshake":
            run = dataclasses.replace(run, unsupported=HANDSHAKE_CLOSES)
        if run.unsupported:
            report.unsupported(run, run.unsupported)
            continue
        exchanges = exchanges_of(run)
        probe = exchanges[0][0]
        try:
            status = control.request(probe.verb, probe.url, probe.body, headers=probe.headers, **options).status_code
        except ucall.Error:
            status = None  # a failed probe is no missing route, so the run measures the failure
        if status in UNSUPPORTED:
            report.unsupported(run, arguments.NOT_SERVED)
            continue
        server_before, client_before = server_stats(control), stats.process_cpu()
        load = pickle.dumps((run, exchanges, options))
        for inbox in inboxes:
            inbox.put(load)
        for _ in workers:
            outbox.get()
        window = stats.Window.starting(
            arguments.start_at(position), args.warmup, args.time_limit, args.calls_per_second, run.in_flight
        )
        opens_before = stats.tcp_active_opens()
        load = pickle.dumps(window)
        for inbox in inboxes:
            inbox.put(load)
        tally = stats.Tally()
        for _ in workers:
            tally.merge(outbox.get())
        opens_after, server_after = stats.tcp_active_opens(), server_stats(control)
        sampled.wait()
        sample = {
            "server_before": server_before,
            "server_after": server_after,
            "client_cpu": stats.process_cpu() - client_before,
            "client_opens": [opens_before, opens_after],
        }
        report.add(run, window, tally, sample)
        if args.between_runs == "reconnect":
            earlier = (stats.sockets_in_use(), server_stats(control)["memory_current_bytes"])
            for _ in range(stats.SETTLE_ROUNDS):
                time.sleep(stats.SETTLE_INTERVAL_SECONDS)
                later = (stats.sockets_in_use(), server_stats(control)["memory_current_bytes"])
                if stats.settled(earlier, later):
                    break
                earlier = later
    for inbox in inboxes:
        inbox.put(None)
    for worker in workers:
        worker.join()
