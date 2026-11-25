"""UCall's Python client driving a UCall server, one client per thread on free-threaded CPython.

The threads live across configurations, so their clients can stay open under `--between-runs keep`; each opens its
share of the connections up front by concurrent health checks, then drives its slots on the schedule `stats.slot_loop`
keeps, waiting on the network exactly until the next call is due. UCall's client binds neither a device nor an address,
so the kernel routes each target by its subnet.
"""

import heapq
import json
import pathlib
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
contexts = (
    {size: payloads.context_body(args.seed, size) for size in args.body_bytes} if "context" in args.workloads else {}
)
options = {"security": security, "noise_peer_key": server_public}


def open_client(connections: int) -> ucall.Client:
    """A client of `connections` connections, admitting the servers' key under Noise XX."""
    authorize = (lambda key: key == server_public) if noise else None
    return ucall.Client(connections=connections, noise_private_key=client_key, peer_authorizer=authorize)


class Exchange:
    """One call as every thread sends it to one target, and the reply it must get back byte for byte."""

    def __init__(self, verb: str, url: str, body: bytes, headers: tuple[str, ...], reply: bytes) -> None:
        self.verb, self.url, self.body, self.headers, self.reply = verb, url, body, headers, reply


def exchanges_of(run: arguments.Run) -> list[list[Exchange]]:
    """Per target, one exchange per ring entry, or the one context upload."""
    closing = (CLOSE,) if run.phase == "handshake" else ()
    if run.workload == "context":
        body = contexts[run.body_bytes]
        reply = payloads.count_reply(len(body))
        return [[Exchange("PUT", f"{origin}/context", body, HEADERS + closing, reply)] for origin in origins]
    calls = payloads.exchanges(ring, run, args.width)

    def headers_of(call: payloads.Exchange) -> tuple[str, ...]:
        traced = (f"authorization: {payloads.AUTHORIZATION}", f"traceparent: {call.traceparent}")
        return HEADERS + (traced if call.traceparent else ()) + closing

    return [
        [Exchange("POST", origin + call.path, call.body, headers_of(call), call.reply) for call in calls]
        for origin in origins
    ]


def get(control: ucall.Client, target: int, path: str) -> ucall.Response:
    return control.get(origins[target] + path, headers=(IDENTITY, CLOSE), **options)


def server_stats(control: ucall.Client) -> dict:
    """The servers' cgroup counters, summed over the targets, and the largest box-wide `tcp_active_opens`."""
    replies = [json.loads(bytes(get(control, target, "/stats").body)) for target in range(len(origins))]

    def total(key: str):
        values = [reply[key] for reply in replies]
        return None if None in values else max(values) if key == "tcp_active_opens" else sum(values)

    return {key: total(key) for key in replies[0]}


def record(bins: list[int], seconds: float) -> None:
    bins[min(stats.bin_of(max(int(seconds * 1e9), 0)), stats.BINS - 1)] += 1


def outcome_of(completed: ucall.Completed, exchange: Exchange) -> tuple[int, int] | str:
    """The body bytes sent and received by a finished call, or one of `stats.FAILURES`."""
    response = completed.response
    if response is None:
        return "timeout" if completed.status == ucall.Status.TIMEOUT else "connect"
    if response.status_code != 200:
        return "status"
    return (len(exchange.body), len(exchange.reply)) if response.body == exchange.reply else "wrong_reply"


def measure(client: ucall.Client, exchanges: list[list[Exchange]], window: stats.Window, index: int, threads: int):
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
                client.submit(exchange.verb, exchange.url, exchange.body, exchange.headers, context=position, **options)
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
    kept, limit = None, 0
    while (load := inbox.get()) is not None:
        run, exchanges = load
        connections = max(arguments.share(run.connections, index, threads), 1)
        if kept is None or limit != connections:
            kept = None
            kept, limit = open_client(connections), connections
        if run.phase == "steady":
            for target, origin in enumerate(origins):
                for _ in range(arguments.share(connections, target, len(origins))):
                    kept.submit("GET", origin + "/health", headers=(IDENTITY,), **options)
            answered = 0
            while answered < connections:
                answered += len(kept.take_completed(CALL_TIMEOUT_MICROSECONDS, connections))
        outbox.put(None)
        window = inbox.get()
        outbox.put(measure(kept, exchanges, window, index, threads))
        sampled.wait()
        if args.between_runs == "reconnect":
            kept = None


if __name__ == "__main__":
    control = open_client(len(origins))
    health = json.loads(bytes(get(control, 0, "/health").body))
    report = results.Report(args, health["framework"], health["client"])
    threads = args.threads
    sampled = threading.Barrier(threads + 1)
    inboxes, outbox = [queue.SimpleQueue() for _ in range(threads)], queue.SimpleQueue()
    workers = [
        threading.Thread(target=work, args=(index, threads, inbox, outbox, sampled))
        for index, inbox in enumerate(inboxes)
    ]
    for worker in workers:
        worker.start()
    for position, run in enumerate(arguments.runs(args, health["batching"], multiplexed=False)):
        exchanges = exchanges_of(run)
        probe = exchanges[0][0]
        status = control.request(probe.verb, probe.url, probe.body, headers=probe.headers, **options).status_code
        if status in UNSUPPORTED:
            report.unsupported(run)
            continue
        server_before, client_before = server_stats(control), stats.process_cpu()
        for inbox in inboxes:
            inbox.put((run, exchanges))
        for _ in workers:
            outbox.get()
        window = stats.Window.starting(
            arguments.start_at(position), args.warmup, args.time_limit, args.calls_per_second, run.in_flight
        )
        opens_before = stats.tcp_active_opens()
        for inbox in inboxes:
            inbox.put(window)
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
            earlier = (stats.sockets_in_use(), server_stats(control)["memory_bytes"])
            for _ in range(stats.SETTLE_ROUNDS):
                time.sleep(stats.SETTLE_INTERVAL_SECONDS)
                later = (stats.sockets_in_use(), server_stats(control)["memory_bytes"])
                if stats.settled(earlier, later):
                    break
                earlier = later
    for inbox in inboxes:
        inbox.put(None)
    for worker in workers:
        worker.join()
