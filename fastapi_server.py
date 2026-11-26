"""FastAPI on Uvicorn serving the tool, context, download and fan-out workloads, one worker process per thread."""

import asyncio
import itertools
from typing import Annotated

import httpx
import uvicorn
from fastapi import FastAPI, Header, Request, Response
from pydantic import BaseModel

import arguments
import payloads
import security
import stats

args = arguments.server_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "fastapi")
interface = arguments.single_interface(args, "fastapi")
scheme = "https" if args.security == "tls" else "http"
tools = [
    f"{scheme}://[{host}]:{port}/tool" if ":" in host else f"{scheme}://{host}:{port}/tool"
    for host, port in args.upstreams or []
]
unlimited = httpx.Limits(max_connections=None, max_keepalive_connections=None)
upstream = httpx.AsyncClient(verify=security.client_context(args, ["http/1.1"]) or True, limits=unlimited, timeout=None)
for name in ("accept", "connection", "user-agent"):
    del upstream.headers[name]
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


@app.post("/raw")
async def raw(request: Request) -> Response:
    return Response((await request.body())[: payloads.REPLY_BYTES], media_type="application/octet-stream")


@app.post("/tool")
async def tool(call: ToolCall) -> Response:
    return Response(payloads.tool_reply(call.id, call.check()), media_type="application/json")


@app.post("/tool/{name}")
async def full(
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
    return Response(payloads.full_reply(call.id, name, trace, lang, after + call.top_k), media_type="application/json")


@app.put("/context")
async def context(request: Request) -> Response:
    count = 0
    async for chunk in request.stream():
        count += len(chunk)
    return Response(payloads.count_reply(count), media_type="text/plain")


@app.get("/context")
async def download(bytes: int) -> Response:
    """The first `bytes` bytes of the body each worker prepares on its first download, which Uvicorn writes as the
    socket drains; 416 when the server prepared fewer, 500 when it could not prepare any."""
    prepared = arguments.download_body(args)
    if prepared is None:
        return Response(status_code=500)
    body = payloads.download(prepared, bytes)
    if body is None:
        return Response(status_code=416)
    return Response(body, media_type="application/octet-stream")


@app.post("/fanout")
async def fanout(request: Request, width: int) -> Response:
    body = await request.body()
    call = ToolCall.model_validate_json(body)
    expected = payloads.tool_reply(call.id, call.check())
    targets = itertools.islice(itertools.cycle(tools), width)
    replies = await asyncio.gather(*(upstream.post(url, content=body, headers=payloads.HEADERS) for url in targets))
    if not tools or not all(reply.status_code == 200 and reply.content == expected for reply in replies):
        return Response(status_code=502)
    return Response(payloads.fanout_reply(call.id, width), media_type="application/json")


@app.get("/stats")
async def unit() -> Response:
    return Response(stats.unit_json(), media_type="application/json")


@app.get("/health")
async def health() -> Response:
    return Response(payloads.health("fastapi", "httpx", None), media_type="application/json")


if __name__ == "__main__":
    listener = arguments.listen(interface, args.port, arguments.admission(args))
    uvicorn.run(
        "fastapi_server:app",
        fd=listener.fileno(),
        workers=args.threads,
        loop="uvloop",
        http="httptools",
        access_log=False,
        timeout_keep_alive=arguments.KEEP_ALIVE_SECONDS,
        # Uvicorn listens again with its own backlog, 2048 unless given.
        backlog=arguments.admission(args),
        **security.uvicorn_options(args),
    )
