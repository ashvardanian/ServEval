"""httpx driving FastAPI, LitServe and Ray Serve, one asyncio loop in each of --threads processes.

The framework and its batching come from the server's /health. httpx binds a local address but no device.
The processes live across configurations, so their pools can stay open under `--between-runs keep`.
"""

import asyncio
import multiprocessing
import time

import httpx

import arguments
import payloads
import results
import security
import stats

args = arguments.client_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "httpx")
arguments.require_no_device(args, "httpx")
scheme = "https" if args.security == "tls" else "http"
bases = [
    f"{scheme}://[{target}]:{args.port}" if ":" in target else f"{scheme}://{target}:{args.port}"
    for target in args.targets
]
ring = payloads.tool_ring(args.seed)
contexts = (
    {size: payloads.context_body(args.seed, size) for size in args.body_bytes} if "context" in args.workloads else {}
)
timeout = args.warmup + args.time_limit
UNSUPPORTED = (404, 405, 501)


def client(target: int, connections: int, tls) -> httpx.AsyncClient:
    """A client from the interface paired with `target`, holding at most `connections` connections, sending only the
    headers every other client sends."""
    local = arguments.interface_of(args, target).addresses[0]
    limits = httpx.Limits(max_connections=connections, max_keepalive_connections=connections)
    transport = httpx.AsyncHTTPTransport(local_address=local, verify=tls or True, limits=limits)
    opened = httpx.AsyncClient(transport=transport, base_url=bases[target], timeout=timeout, headers=payloads.HEADERS)
    for name in ("accept", "connection", "user-agent"):
        del opened.headers[name]
    return opened


def request(
    run: arguments.Run, exchanges: list[payloads.Exchange], sequence: int
) -> tuple[str, str, bytes, dict, bytes]:
    """The method, path, body and headers of call `sequence`, and the reply it must get back."""
    if run.workload == "context":
        body = contexts[run.body_bytes]
        return "PUT", "/context", body, {}, payloads.count_reply(len(body))
    exchange = exchanges[sequence % payloads.RING]
    headers = {}
    if exchange.traceparent:
        headers = {"authorization": payloads.AUTHORIZATION, "traceparent": exchange.traceparent}
    return "POST", exchange.path, exchange.body, headers, exchange.reply


def exchanges_of(run: arguments.Run) -> list[payloads.Exchange]:
    return [] if run.workload == "context" else payloads.exchanges(ring, run, args.width)


async def measure(run: arguments.Run, index: int, processes: int, tls, clients: list[httpx.AsyncClient]):
    """Runs this process's slots `index + i * processes`, slot `i` on target `i % targets`, so each target's pool
    holds at least as many connections as the slots it carries; process 0 samples the box and the servers before any
    connection closes."""
    loop = asyncio.get_running_loop()
    exchanges = exchanges_of(run)
    await loop.run_in_executor(None, connected.wait)
    window = stats.Window.starting(begin.value, args.warmup, args.time_limit, args.calls_per_second, run.in_flight)
    opens_before = stats.tcp_active_opens()
    suites = []

    def caller(slot: int):
        target = slot // processes % len(bases)
        closing = {"connection": "close"} if run.phase == "handshake" else {}

        async def call(sequence: int) -> tuple[int, int] | str:
            method, path, body, headers, expected = request(run, exchanges, sequence)
            try:
                if run.phase == "steady":
                    response = await clients[target].request(method, path, content=body, headers=headers | closing)
                else:
                    async with client(target, 1, tls) as fresh:
                        response = await fresh.request(method, path, content=body, headers=headers | closing)
            except httpx.TimeoutException:
                return "timeout"
            except (httpx.HTTPError, OSError) as error:
                return stats.failure_of(error)
            ssl_object = response.extensions["network_stream"].get_extra_info("ssl_object") if tls else None
            if ssl_object and not suites:
                suites.append(ssl_object.cipher()[0])
            if response.status_code != 200:
                return "status"
            return (len(body), len(response.content)) if response.content == expected else "wrong_reply"

        return call

    slots = range(index, run.in_flight, processes)
    tallies = await asyncio.gather(*(stats.slot_loop(window, slot, caller(slot)) for slot in slots))
    await loop.run_in_executor(None, barrier.wait)
    sampled = ([opens_before, stats.tcp_active_opens()], server_stats(tls)) if index == 0 else None
    await loop.run_in_executor(None, barrier.wait)
    tally = stats.Tally()
    for other in tallies:
        tally.merge(other)
    return tally, (suites or [None])[0], sampled


async def drive(index: int, processes: int, inbox, outbox) -> None:
    """Measures every run from `inbox` over one pool per target, which stays open across runs under `keep`, and
    sends each outcome to `outbox` with this process's CPU seconds."""
    tls = security.client_context(args, ["http/1.1"])
    loop = asyncio.get_running_loop()
    clients, limits = [], []
    while (run := await loop.run_in_executor(None, inbox.get)) is not None:
        cpu_before = stats.process_cpu()
        connections = arguments.share(run.connections, index, processes)
        wanted = [max(arguments.share(connections, target, len(bases)), 1) for target in range(len(bases))]
        if run.phase == "steady" and wanted != limits:
            for pooled in clients:
                await pooled.aclose()
            clients, limits = [client(target, limit, tls) for target, limit in enumerate(wanted)], wanted
        if run.phase == "steady":
            for pooled, limit in zip(clients, limits):
                await asyncio.gather(*(pooled.get("/health") for _ in range(limit)))
        tally, suite, sampled = await measure(run, index, processes, tls, clients)
        if args.between_runs == "reconnect":
            for pooled in clients:
                await pooled.aclose()
            clients, limits = [], []
        outbox.put((index, tally, suite, sampled, stats.process_cpu() - cpu_before))


def work(index: int, processes: int, inbox, outbox) -> None:
    asyncio.run(drive(index, processes, inbox, outbox))


def server_stats(tls) -> dict:
    """The servers' cgroup counters, summed over the targets, and the largest box-wide `tcp_active_opens`."""
    replies = [httpx.get(f"{base}/stats", headers=payloads.HEADERS, verify=tls or True).json() for base in bases]

    def total(key: str):
        values = [reply[key] for reply in replies]
        return None if None in values else max(values) if key == "tcp_active_opens" else sum(values)

    return {key: total(key) for key in replies[0]}


def probe(run: arguments.Run, tls) -> int:
    """The status of one call on the run's route, telling a missing route from a working one."""
    method, path, body, headers, _ = request(run, exchanges_of(run), 0)
    headers = payloads.HEADERS | headers
    return httpx.request(method, bases[0] + path, content=body, headers=headers, verify=tls or True).status_code


if __name__ == "__main__":
    tls = security.client_context(args, ["http/1.1"])
    healths = [httpx.get(f"{base}/health", headers=payloads.HEADERS, verify=tls or True).json() for base in bases]
    framework, batching = healths[0]["framework"], all(health["batching"] for health in healths)
    report = results.Report(args, framework, "httpx")
    processes = args.threads
    forking = multiprocessing.get_context("fork")
    begin, number = forking.Value("d"), forking.Value("q")
    # The last process to finish connecting starts the window, so connection setup is never measured.
    connected = forking.Barrier(processes, action=lambda: setattr(begin, "value", arguments.start_at(number.value)))
    barrier = forking.Barrier(processes)
    inboxes, outbox = [forking.SimpleQueue() for _ in range(processes)], forking.SimpleQueue()
    workers = [
        forking.Process(target=work, args=(index, processes, inbox, outbox)) for index, inbox in enumerate(inboxes)
    ]
    for worker in workers:
        worker.start()
    for position, run in enumerate(arguments.runs(args, batching, multiplexed=False)):
        if probe(run, tls) in UNSUPPORTED:
            report.unsupported(run)
            continue
        server_before, client_before = server_stats(tls), stats.process_cpu()
        number.value = position
        for inbox in inboxes:
            inbox.put(run)
        outcomes = sorted((outbox.get() for _ in workers), key=lambda outcome: outcome[0])
        tally = stats.Tally()
        for _, other, _, _, _ in outcomes:
            tally.merge(other)
        client_opens, server_after = outcomes[0][3]
        sample = {
            "server_before": server_before,
            "server_after": server_after,
            "client_cpu": stats.process_cpu() - client_before + sum(outcome[4] for outcome in outcomes),
            "client_opens": client_opens,
            "tls_suite": next((suite for _, _, suite, _, _ in outcomes if suite), None),
        }
        window = stats.Window.starting(begin.value, args.warmup, args.time_limit, args.calls_per_second, run.in_flight)
        report.add(run, window, tally, sample)
        if args.between_runs == "reconnect":
            earlier = (stats.sockets_in_use(), server_stats(tls)["memory_bytes"])
            for _ in range(stats.SETTLE_ROUNDS):
                time.sleep(stats.SETTLE_INTERVAL_SECONDS)
                later = (stats.sockets_in_use(), server_stats(tls)["memory_bytes"])
                if stats.settled(earlier, later):
                    break
                earlier = later
    for inbox in inboxes:
        inbox.put(None)
    for worker in workers:
        worker.join()
