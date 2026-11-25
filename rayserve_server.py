"""Ray Serve with a FastAPI ingress serving the tool, context and fan-out workloads, batching through serve.batch.

Ray's proxy binds by address alone, so it cannot pin a device; --threads sets the replicas.
"""

import asyncio
import itertools
import os
import pathlib
from typing import Annotated

import httpx
import ray
from fastapi import FastAPI, Header, Request, Response
from pydantic import BaseModel
from ray import serve

import arguments
import payloads
import security
import stats

args = arguments.server_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "rayserve")
arguments.require_no_device(args, "rayserve")
interface = arguments.single_interface(args, "rayserve")
scheme = "https" if args.security == "tls" else "http"
tools = [
    f"{scheme}://[{host}]:{port}/tool" if ":" in host else f"{scheme}://{host}:{port}/tool"
    for host, port in args.upstreams or []
]
app = FastAPI()


class ToolFilters(BaseModel):
    lang: str
    after: int


class ToolArguments(BaseModel):
    query: str
    top_k: int
    filters: ToolFilters


class ToolCall(BaseModel):
    id: str
    type: str
    name: str
    arguments: ToolArguments
    trace: str

    def check(self) -> int:
        return self.arguments.filters.after + self.arguments.top_k


class FullCall(BaseModel):
    id: str
    query: str
    top_k: int
    trace: str


@serve.deployment(max_ongoing_requests=arguments.admission(args))
@serve.ingress(app)
class Agent:
    def __init__(self) -> None:
        unlimited = httpx.Limits(max_connections=None, max_keepalive_connections=None)
        verify = security.client_context(args, ["http/1.1"]) or True
        self.upstream = httpx.AsyncClient(verify=verify, limits=unlimited, timeout=None)
        for name in ("accept", "connection", "user-agent"):
            del self.upstream.headers[name]

    async def fan_out(self, body: bytes, expected: bytes, width: int) -> bool:
        targets = itertools.islice(itertools.cycle(tools), width)
        replies = await asyncio.gather(
            *(self.upstream.post(url, content=body, headers=payloads.HEADERS) for url in targets)
        )
        return bool(tools) and all(reply.status_code == 200 and reply.content == expected for reply in replies)

    @serve.batch(max_batch_size=args.batch_size, batch_wait_timeout_s=args.batch_wait)
    async def batched_tool(self, calls: list[tuple[str, int]]) -> list[bytes]:
        return [payloads.tool_reply(id, check) for id, check in calls]

    @serve.batch(max_batch_size=args.batch_size, batch_wait_timeout_s=args.batch_wait)
    async def batched_fan_out(self, calls: list[tuple[bytes, bytes, int]]) -> list[bool]:
        return await asyncio.gather(*(self.fan_out(*call) for call in calls))

    @app.post("/raw")
    async def raw(self, request: Request) -> Response:
        return Response((await request.body())[: payloads.REPLY_BYTES], media_type="application/octet-stream")

    @app.post("/tool")
    async def tool(self, call: ToolCall) -> Response:
        return Response(payloads.tool_reply(call.id, call.check()), media_type="application/json")

    @app.post("/tool/batch")
    async def tool_batch(self, call: ToolCall) -> Response:
        return Response(await self.batched_tool((call.id, call.check())), media_type="application/json")

    @app.post("/tool/{name}")
    async def full(
        self,
        name: str,
        lang: str,
        after: int,
        call: FullCall,
        authorization: Annotated[str | None, Header()] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ) -> Response:
        if authorization != payloads.AUTHORIZATION:
            return Response(status_code=401)
        trace = payloads.trace_id(traceparent)
        if trace is None:
            return Response(status_code=400)
        reply = payloads.full_reply(call.id, name, trace, lang, after + call.top_k)
        return Response(reply, media_type="application/json")

    @app.put("/context")
    async def context(self, request: Request) -> Response:
        count = 0
        async for chunk in request.stream():
            count += len(chunk)
        return Response(payloads.count_reply(count), media_type="text/plain")

    @app.post("/fanout")
    async def fanout(self, request: Request, width: int) -> Response:
        body = await request.body()
        call = ToolCall.model_validate_json(body)
        if not await self.fan_out(body, payloads.tool_reply(call.id, call.check()), width):
            return Response(status_code=502)
        return Response(payloads.fanout_reply(call.id, width), media_type="application/json")

    @app.post("/fanout/batch")
    async def fanout_batch(self, request: Request, width: int) -> Response:
        body = await request.body()
        call = ToolCall.model_validate_json(body)
        if not await self.batched_fan_out((body, payloads.tool_reply(call.id, call.check()), width)):
            return Response(status_code=502)
        return Response(payloads.fanout_reply(call.id, width), media_type="application/json")

    @app.get("/stats")
    async def unit(self) -> Response:
        return Response(stats.unit_json(), media_type="application/json")

    @app.get("/health")
    async def health(self) -> Response:
        return Response(payloads.health("rayserve", "httpx", True), media_type="application/json")


if __name__ == "__main__":
    # Replicas import the shared helpers, so they need this directory on their path. Each process starts its own Ray
    # instance on its own cores rather than joining another's; Ray picks free ports and a session directory for each.
    ray.init(
        address="local",
        num_cpus=len(os.sched_getaffinity(0)),
        runtime_env={"env_vars": {"PYTHONPATH": str(pathlib.Path(__file__).resolve().parent)}},
    )
    http = {"host": interface.addresses[0], "port": args.port, "keep_alive_timeout_s": arguments.KEEP_ALIVE_SECONDS}
    serve.start(http_options={**http, **security.uvicorn_options(args)})
    serve.run(Agent.options(num_replicas=args.threads).bind(), blocking=True)
