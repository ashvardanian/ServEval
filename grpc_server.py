"""Python's asynchronous gRPC serving the tool, context, download and fan-out workloads, one so_reuseport process per
thread.

gRPC binds by address alone, so it cannot pin a device.
"""

import asyncio
import contextlib
import itertools
import multiprocessing
import signal

import grpc

import arguments
import payloads
import security
import stats

protos, services = grpc.protos_and_services("serveval.proto")
args = arguments.server_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "grpc")
arguments.require_no_device(args, "grpc")
interface = arguments.single_interface(args, "grpc")
keys = security.certificate_bytes(args)
OPTIONS = (("grpc.max_receive_message_length", -1), ("grpc.max_send_message_length", -1))
PADDING = b"x" * payloads.MESSAGE_CONTENT_BYTES
# How long a stopping server lets the calls it already took finish.
STOP_GRACE_SECONDS = 1.0


def address(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def reply(call, trace: str = ""):
    """The reply copying the call's id, `after + top_k` and a `Full` call's trace id, padded to `REPLY_BYTES`."""
    padding = PADDING[: payloads.MESSAGE_CONTENT_BYTES - len(trace) - 2] if trace else PADDING
    check = call.arguments.filters.after + call.arguments.top_k
    return protos.ToolReply(id=call.id, ok=True, content=padding, check=check, trace=trace)


class Agent(services.AgentServicer):
    """Answers every workload from the decoded message alone, fanning out over one channel per upstream."""

    def __init__(self) -> None:
        credentials = None if keys is None else grpc.ssl_channel_credentials(root_certificates=keys[2])
        channels = [
            grpc.aio.insecure_channel(address(*upstream), options=OPTIONS)
            if credentials is None
            else grpc.aio.secure_channel(address(*upstream), credentials, options=OPTIONS)
            for upstream in args.upstreams or []
        ]
        self.upstreams = [services.AgentStub(channel) for channel in channels]
        # The prepared download body as the messages that stream it, made on the first download.
        self.chunks = []

    async def Tool(self, request, context):
        return reply(request)

    async def Raw(self, request, context):
        return protos.Chunk(data=request.data[: payloads.REPLY_BYTES])

    async def Full(self, request, context):
        metadata = dict(context.invocation_metadata())
        if metadata.get("authorization") != payloads.AUTHORIZATION:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "the bearer token does not match")
        trace = payloads.trace_id(metadata.get("traceparent"))
        if trace is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the traceparent metadata entry does not parse")
        return reply(request, trace)

    async def Context(self, request_iterator, context):
        count = 0
        async for chunk in request_iterator:
            count += len(chunk.data)
        return protos.Count(bytes=count)

    async def Download(self, request, context):
        """Streams the chunks of the body this process prepares on its first download that cover the first `bytes`
        bytes, the last one cut to fit."""
        prepared = arguments.download_body(args)
        if prepared is None:
            await context.abort(grpc.StatusCode.INTERNAL, "the server prepared no download body")
        if request.bytes > len(prepared):
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, "the server prepared fewer download bytes")
        if not self.chunks:
            self.chunks = [
                protos.Chunk(data=prepared[offset : offset + payloads.CHUNK_BYTES])
                for offset in range(0, len(prepared), payloads.CHUNK_BYTES)
            ]
        whole, rest = divmod(request.bytes, payloads.CHUNK_BYTES)
        for chunk in self.chunks[:whole]:
            yield chunk
        if rest:
            yield protos.Chunk(data=prepared[whole * payloads.CHUNK_BYTES : request.bytes])

    async def Fanout(self, request, context):
        width = dict(context.invocation_metadata()).get("width", "")
        if not (width.isascii() and width.isdigit()):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the width metadata entry is missing")
        if not self.upstreams:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "started without --upstreams")
        expected = reply(request)
        targets = itertools.islice(itertools.cycle(self.upstreams), int(width))
        replies = await asyncio.gather(*(upstream.Tool(request) for upstream in targets))
        if not all(upstream_reply == expected for upstream_reply in replies):
            await context.abort(grpc.StatusCode.DATA_LOSS, "an upstream reply does not match the call")
        return expected

    async def Health(self, request, context):
        # `HealthReply` has no field for the stream limit, so it travels as metadata, left out for no limit: the
        # `grpc.max_concurrent_streams` channel option the server is built with, which these options do not set.
        streams = dict(OPTIONS).get("grpc.max_concurrent_streams")
        if streams is not None:
            await context.send_initial_metadata(((payloads.MAX_CONCURRENT_STREAMS_METADATA, str(streams)),))
        return protos.HealthReply(framework="grpc", client="grpc.aio", batching=False)

    async def Stats(self, request, context):
        fields, metadata = stats.grpc_reply(stats.unit_stats())
        await context.send_initial_metadata(metadata)
        return protos.StatsReply(**fields)


async def serve() -> None:
    """Serves until SIGINT or SIGTERM; every process binds the same port and the kernel spreads connections."""
    server = grpc.aio.server(options=(*OPTIONS, ("grpc.so_reuseport", 1)))
    services.add_AgentServicer_to_server(Agent(), server)
    bound = address(interface.addresses[0], args.port)
    if keys is None:
        server.add_insecure_port(bound)
    else:
        server.add_secure_port(bound, grpc.ssl_server_credentials([(keys[1], keys[0])]))
    await server.start()
    stopping = asyncio.Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(number, stopping.set)
    try:
        await stopping.wait()
    finally:
        await server.stop(STOP_GRACE_SECONDS)


def run() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve())


if __name__ == "__main__":
    workers = [multiprocessing.Process(target=run) for _ in range(args.threads - 1)]
    for worker in workers:
        worker.start()
    try:
        run()
    finally:
        # SIGTERM stops each worker's server as SIGINT or SIGTERM stopped this one.
        for worker in workers:
            worker.terminate()
            worker.join()
