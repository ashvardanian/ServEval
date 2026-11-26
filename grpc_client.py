"""grpc.aio driving grpc_server.py, one asyncio loop in each of --threads processes.

Each process holds one channel per connection, many calls multiplexed on each.
grpc.aio exposes no local address or device, so --interfaces only pairs with --targets by position.
The processes live across configurations, so their channels can stay open under `--between-runs keep`.
"""

import asyncio
import multiprocessing

import grpc

import arguments
import payloads
import results
import security
import stats

protos, services = grpc.protos_and_services("serveval.proto")
args = arguments.client_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "grpc.aio")
arguments.require_no_device(args, "grpc.aio")
keys = security.certificate_bytes(args)
credentials = None if keys is None else grpc.ssl_channel_credentials(root_certificates=keys[2])
targets = [f"[{target}]:{args.port}" if ":" in target else f"{target}:{args.port}" for target in args.targets]
# A private subchannel pool keeps channels to one target from sharing a connection.
OPTIONS = (
    ("grpc.use_local_subchannel_pool", 1),
    ("grpc.max_receive_message_length", -1),
    ("grpc.max_send_message_length", -1),
)
ring = payloads.tool_ring(args.seed)
messages = [
    protos.ToolCall(
        id=parts.id,
        type="function",
        name=payloads.TOOL_NAME,
        arguments=protos.Arguments(
            query=parts.query, top_k=parts.top_k, filters=protos.Filters(lang=payloads.LANG, after=parts.after)
        ),
        trace=parts.trace[: payloads.MESSAGE_TRACE_BYTES],
    )
    for parts in ring
]
bodies = [parts.body() for parts in ring]
PADDING = b"x" * payloads.MESSAGE_CONTENT_BYTES
replies = [protos.ToolReply(id=parts.id, ok=True, content=PADDING, check=parts.check(), trace="") for parts in ring]
full_replies = [
    protos.ToolReply(
        id=parts.id,
        ok=True,
        content=PADDING[: payloads.MESSAGE_CONTENT_BYTES - len(parts.trace_id()) - 2],
        check=parts.check(),
        trace=parts.trace_id(),
    )
    for parts in ring
]
full_metadata = [(("authorization", payloads.AUTHORIZATION), ("traceparent", parts.traceparent())) for parts in ring]
assert all(
    message.ByteSize() == payloads.TOOL_BYTES and reply.ByteSize() == full.ByteSize() == payloads.REPLY_BYTES
    for message, reply, full in zip(messages, replies, full_replies)
), "the tool messages drifted from 1024 and 256 bytes"
# The body each upload sends and each download must get back, per size.
bodies = {"context", "download"} & set(args.workloads)
contexts = {size: payloads.context_body(args.seed, size) for size in arguments.body_bytes(args)} if bodies else {}
chunks = {
    size: [
        protos.Chunk(data=body[offset : offset + payloads.CHUNK_BYTES])
        for offset in range(0, size, payloads.CHUNK_BYTES)
    ]
    for size, body in contexts.items()
    if "context" in args.workloads
}
WIDTH = (("width", str(args.width)),)


def channel(target: int) -> grpc.aio.Channel:
    if credentials is None:
        return grpc.aio.insecure_channel(targets[target], options=OPTIONS)
    return grpc.aio.secure_channel(targets[target], credentials, options=OPTIONS)


async def call_once(run: arguments.Run, stub, sequence: int) -> tuple[int, int] | str:
    """One call of the run's workload, with the payload bytes sent and received when the reply matches."""
    if run.workload == "context":
        reply = await stub.Context(iter(chunks[run.body_bytes]))
        return (run.body_bytes, reply.ByteSize()) if reply.bytes == run.body_bytes else "wrong_reply"
    if run.workload == "download":
        received = [chunk.data async for chunk in stub.Download(protos.Count(bytes=run.body_bytes))]
        body = b"".join(received)
        return (0, len(body)) if body == contexts[run.body_bytes] else "wrong_reply"
    index = sequence % payloads.RING
    sizes = (payloads.TOOL_BYTES, payloads.REPLY_BYTES)
    if run.workload == "fanout":
        valid = await stub.Fanout(messages[index], metadata=WIDTH) == replies[index]
    elif run.parsing == "raw":
        body = bodies[index]
        reply = await stub.Raw(protos.Chunk(data=body))
        valid, sizes = reply.data == body[: payloads.REPLY_BYTES], (len(body), len(reply.data))
    elif run.parsing == "full":
        valid = await stub.Full(messages[index], metadata=full_metadata[index]) == full_replies[index]
    else:
        valid = await stub.Tool(messages[index]) == replies[index]
    return sizes if valid else "wrong_reply"


def failure(error: grpc.aio.AioRpcError) -> str:
    """Why a call failed: a refused or other transport failure, a deadline, or a status the server sent."""
    if error.code() == grpc.StatusCode.UNAVAILABLE:
        return "refused" if "refused" in (error.details() or "").lower() else "connect"
    return "timeout" if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED else "status"


async def server_stats(stubs) -> dict:
    """The servers' `Stats` replies as one server's, as `stats.server_stats` sums them."""
    replies = []
    for stub in stubs:
        call = stub.Stats(protos.Empty())
        replies.append(stats.from_grpc_reply(await call, await call.initial_metadata()))
    return stats.server_stats(replies)


async def measure(run: arguments.Run, index: int, processes: int, stubs, channels: list[list[grpc.aio.Channel]]):
    """Runs this process's slots `index + i * processes`, slot `i` on target `i % targets`, so each target's channels
    carry the slots evenly; process 0 samples the box and the servers before any closes."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, connected.wait)
    window = stats.Window.starting(begin.value, args.warmup, args.time_limit, args.calls_per_second, run.in_flight)
    opens_before = stats.tcp_active_opens()

    def caller(slot: int):
        target = slot // processes % len(targets)

        async def call(sequence: int) -> tuple[int, int] | str:
            try:
                if run.phase == "steady":
                    pool = channels[target]
                    opened = pool[slot // processes // len(targets) % len(pool)]
                    return await call_once(run, services.AgentStub(opened), sequence)
                async with channel(target) as fresh:
                    return await call_once(run, services.AgentStub(fresh), sequence)
            except grpc.aio.AioRpcError as error:
                return failure(error)

        return call

    slots = range(index, run.in_flight, processes)
    tallies = await asyncio.gather(*(stats.slot_loop(window, slot, caller(slot)) for slot in slots))
    await loop.run_in_executor(None, barrier.wait)
    sampled = ([opens_before, stats.tcp_active_opens()], await server_stats(stubs)) if index == 0 else None
    await loop.run_in_executor(None, barrier.wait)
    tally = stats.Tally()
    for other in tallies:
        tally.merge(other)
    return tally, sampled


async def drive(index: int, processes: int, inbox, outbox) -> None:
    """Measures every run from `inbox` over one channel per connection, which stay open across runs under `keep`,
    and sends each outcome to `outbox` with this process's CPU seconds."""
    loop = asyncio.get_running_loop()
    stubs = [services.AgentStub(channel(target)) for target in range(len(targets))]
    channels = [[] for _ in targets]
    while (run := await loop.run_in_executor(None, inbox.get)) is not None:
        cpu_before = stats.process_cpu()
        connections = arguments.share(run.connections, index, processes)
        if run.phase == "steady":
            for target, pool in enumerate(channels):
                share = max(arguments.share(connections, target, len(targets)), 1)
                await asyncio.gather(*(extra.close() for extra in pool[share:]))
                del pool[share:]
                pool += [channel(target) for _ in range(share - len(pool))]
                await asyncio.gather(*(opened.channel_ready() for opened in pool))
        tally, sampled = await measure(run, index, processes, stubs, channels)
        if args.between_runs == "reconnect":
            for pool in channels:
                await asyncio.gather(*(opened.close() for opened in pool))
                pool.clear()
        outbox.put((index, tally, sampled, stats.process_cpu() - cpu_before))


def work(index: int, processes: int, inbox, outbox) -> None:
    asyncio.run(drive(index, processes, inbox, outbox))


async def main() -> None:
    stubs = [services.AgentStub(channel(target)) for target in range(len(targets))]
    checking = stubs[0].Health(protos.Empty())
    health = await checking
    # `HealthReply` has no field for the stream limit, so the server sends it as metadata, absent for no limit.
    advertised = dict(await checking.initial_metadata()).get(payloads.MAX_CONCURRENT_STREAMS_METADATA, "")
    advertised = int(advertised) if advertised.isascii() and advertised.isdigit() else None
    # The row names this client, not the one `Health` names for the server's own pair.
    report = results.Report(args, health.framework, "grpc.aio")
    loop = asyncio.get_running_loop()
    for position, run in enumerate(arguments.runs(args, health.batching, True, advertised)):
        try:
            await call_once(run, stubs[0], 0)
        except grpc.aio.AioRpcError as error:
            if error.code() == grpc.StatusCode.UNIMPLEMENTED:
                report.unsupported(run, arguments.NOT_SERVED)
                continue
        server_before, client_before = await server_stats(stubs), stats.process_cpu()
        number.value = position
        for inbox in inboxes:
            inbox.put(run)
        outcomes = sorted(
            [await loop.run_in_executor(None, outbox.get) for _ in workers], key=lambda outcome: outcome[0]
        )
        tally = stats.Tally()
        for _, other, _, _ in outcomes:
            tally.merge(other)
        client_opens, server_after = outcomes[0][2]
        sample = {
            "server_before": server_before,
            "server_after": server_after,
            "client_cpu": stats.process_cpu() - client_before + sum(outcome[3] for outcome in outcomes),
            "client_opens": client_opens,
        }
        window = stats.Window.starting(begin.value, args.warmup, args.time_limit, args.calls_per_second, run.in_flight)
        report.add(run, window, tally, sample)
        if args.between_runs == "reconnect":
            earlier = (stats.sockets_in_use(), (await server_stats(stubs))["memory_current_bytes"])
            for _ in range(stats.SETTLE_ROUNDS):
                await asyncio.sleep(stats.SETTLE_INTERVAL_SECONDS)
                later = (stats.sockets_in_use(), (await server_stats(stubs))["memory_current_bytes"])
                if stats.settled(earlier, later):
                    break
                earlier = later
    for inbox in inboxes:
        inbox.put(None)


if __name__ == "__main__":
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
    # The workers fork before this process opens a channel, since gRPC's core does not survive a fork.
    for worker in workers:
        worker.start()
    asyncio.run(main())
    for worker in workers:
        worker.join()
