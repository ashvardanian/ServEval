"""LitServe serving the tool workload, plain and through its own batcher.

LitServe hands an API only the decoded JSON body, never a raw stream, the path, the query string or the headers, so
it has no context, fan-out, raw or full routes. It binds by address alone, so it cannot pin a device.
"""

import litserve
from fastapi import Response

import arguments
import payloads
import security
import stats

args = arguments.server_parser(__doc__.partition("\n")[0]).parse_args()
security.require_tls_or_none(args, "litserve")
arguments.require_no_device(args, "litserve")
interface = arguments.single_interface(args, "litserve")


class ToolAPI(litserve.LitAPI):
    """Answers each tool call with the reply copying its id and `after + top_k`, one call or one batch at a time."""

    def setup(self, device: str) -> None:
        pass

    def decode_request(self, request: dict) -> tuple[str, int]:
        arguments = request["arguments"]
        return request["id"], arguments["filters"]["after"] + arguments["top_k"]

    def batch(self, calls: list[tuple[str, int]]) -> list[tuple[str, int]]:
        return calls

    def predict(self, calls):
        return calls

    def unbatch(self, calls: list[tuple[str, int]]) -> list[tuple[str, int]]:
        return calls

    def encode_response(self, call: tuple[str, int]) -> Response:
        return Response(payloads.tool_reply(*call), media_type="application/json")


if __name__ == "__main__":
    batched = ToolAPI(api_path="/tool/batch", max_batch_size=args.batch_size, batch_timeout=args.batch_wait)
    server = litserve.LitServer(
        [ToolAPI(api_path="/tool"), batched],
        accelerator="cpu",
        # Both APIs get this many inference workers each, so together they match the cores, as the other servers do;
        # 2 x 160 workers never became ready within 5 minutes on a 160-core budget.
        workers_per_device=max(1, args.threads // 2),
        healthcheck_path="/ready",
    )
    server.app.add_api_route("/health", lambda: Response(payloads.health("litserve", "httpx", True)), methods=["GET"])
    server.app.add_api_route("/stats", lambda: Response(stats.unit_json()), methods=["GET"])
    server.run(
        host=interface.addresses[0],
        port=args.port,
        generate_client_file=False,
        timeout_keep_alive=arguments.KEEP_ALIVE_SECONDS,
        backlog=arguments.admission(args),
        **security.uvicorn_options(args),
    )
