"""Seeded request bodies and fixed-size replies, byte-identical with `payloads.rs`.

Run it directly to print and check the SHA-256 of tool calls 0 and 1023 with their replies, of the `full` call 0 with
its reply, and of a 256KB context body for seed 42.
"""

import dataclasses
import hashlib
import re
import struct

TOOL_BYTES = 1024
REPLY_BYTES = 256
RING = 1024
MESSAGE_TRACE_BYTES = 888
MESSAGE_CONTENT_BYTES = 222
JSON = {"content-type": "application/json"}
# The headers every HTTP client sends, so no server compresses a reply.
HEADERS = {**JSON, "accept-encoding": "identity"}
TOOL_NAME = "search_documents"
LANG = "en"
AUTHORIZATION = "Bearer 6f1c09a4e2b7d35c81f0aa4e92d1c7b3"
DIGESTS = (
    "69cb58f431dcf185b8fbea14a165f28edc5f80cfbb6f342501bce01d21f45286",
    "671c52debf7b9527575511306acc935b86ffce8f6b3549f5ef38d993d00e9966",
    "9179f5c8c682f517a676366d288fb66636ecd77fddbc1a93acc22764b3431aa1",
    "121da22f71b9cc6e4f5cd9a8654c7767e8513f3e7b40e6ec8f22f8b2c00a72e0",
)

MASK = (1 << 64) - 1
GOLDEN_GAMMA = 0x9E3779B97F4A7C15
QUERY_ALPHABET = b"abcdefghijklmnopqrstuvwxyz      "
HEX_DIGITS = b"0123456789abcdef"
QUERY_BYTES = 64
AFTER_FIRST = 1_767_225_600
AFTER_SPAN = 365 * 86_400
PADDING = b"x" * REPLY_BYTES
TRACEPARENT = re.compile(rb"[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}")


def split_mix_bits(value: int) -> int:
    """SplitMix64's finalizer."""
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)


def stream_key(seed: int, name: str, index: int) -> int:
    """The `index`-th stream of the family `name` under `seed`, as UCall's tools derive it."""
    hashed = 0xCBF29CE484222325
    for byte in name.encode():
        hashed = ((hashed ^ byte) * 0x100000001B3) & MASK
    return split_mix_bits((split_mix_bits(seed ^ hashed) + index) & MASK)


def split_mix_bytes(key: int, count: int) -> bytes:
    """`count` bytes of SplitMix64 draws from `key`, each draw as 8 little-endian bytes."""
    draws, block = (count + 7) // 8, 1 << 17
    blocks = []
    for first in range(1, draws + 1, block):
        steps = range(first, min(first + block, draws + 1))
        words = (split_mix_bits((key + GOLDEN_GAMMA * step) & MASK) for step in steps)
        blocks.append(struct.pack(f"<{len(steps)}Q", *words))
    return b"".join(blocks)[:count]


class ToolParts:
    """The seeded fields of one tool call, shared by its JSON bodies and its protobuf message."""

    def __init__(self, seed: int, index: int) -> None:
        key = stream_key(seed, "tool", index)
        self.id = f"call_{split_mix_bits((key + GOLDEN_GAMMA) & MASK):016x}"
        numbers = split_mix_bits((key + 2 * GOLDEN_GAMMA) & MASK)
        stream = split_mix_bytes((key + 2 * GOLDEN_GAMMA) & MASK, TOOL_BYTES)
        self.query = bytes(QUERY_ALPHABET[byte & 31] for byte in stream[:QUERY_BYTES]).decode()
        self.top_k = 1 + numbers % 64
        self.after = AFTER_FIRST + (numbers >> 32) % AFTER_SPAN
        self.trace = bytes(HEX_DIGITS[byte & 15] for byte in stream[QUERY_BYTES:]).decode()

    def check(self) -> int:
        """What every reply echoes to prove the server read both typed values."""
        return self.after + self.top_k

    def trace_id(self) -> str:
        return self.trace[-32:]

    def traceparent(self) -> str:
        return f"00-{self.trace_id()}-{self.trace[-48:-32]}-01"

    def _padded(self, head: str) -> bytes:
        return f'{head}{self.trace[: TOOL_BYTES - len(head) - 2]}"}}'.encode()

    def body(self) -> bytes:
        """The JSON body, exactly `TOOL_BYTES` long."""
        return self._padded(
            f'{{"id":"{self.id}","type":"function","name":"{TOOL_NAME}","arguments":{{"query":"{self.query}",'
            f'"top_k":{self.top_k},"filters":{{"lang":"{LANG}","after":{self.after}}}}},"trace":"'
        )

    def full_body(self) -> bytes:
        """The `full` call's JSON body, exactly `TOOL_BYTES` long, its name, `lang` and `after` moved to the URL."""
        return self._padded(f'{{"id":"{self.id}","query":"{self.query}","top_k":{self.top_k},"trace":"')

    def full_path(self) -> str:
        return f"/tool/{TOOL_NAME}?lang={LANG}&after={self.after}"


def tool_ring(seed: int) -> list[ToolParts]:
    """The `RING` tool calls of `seed`."""
    return [ToolParts(seed, index) for index in range(RING)]


def context_body(seed: int, count: int) -> bytes:
    """A context upload of `count` seeded bytes."""
    return split_mix_bytes(stream_key(seed, "context", 0), count)


@dataclasses.dataclass(frozen=True)
class Exchange:
    """One HTTP tool or fan-out call as a client sends it, and the reply it must get back byte for byte; a `full`
    call carries its `traceparent`, sent with `AUTHORIZATION`."""

    path: str
    traceparent: str | None
    body: bytes
    reply: bytes


def exchanges(ring: list[ToolParts], run, width: int) -> list[Exchange]:
    """The calls of a tool or fan-out `run`, one per ring entry."""
    batch = "/batch" if run.variant == "batched" else ""

    def exchange(parts: ToolParts) -> Exchange:
        if run.workload == "fanout":
            return Exchange(f"/fanout{batch}?width={width}", None, parts.body(), fanout_reply(parts.id, width))
        if run.parsing == "raw":
            return Exchange("/raw", None, parts.body(), parts.body()[:REPLY_BYTES])
        if run.parsing == "full":
            reply = full_reply(parts.id, TOOL_NAME, parts.trace_id(), LANG, parts.check())
            return Exchange(parts.full_path(), parts.traceparent(), parts.full_body(), reply)
        return Exchange(f"/tool{batch}", None, parts.body(), tool_reply(parts.id, parts.check()))

    return [exchange(parts) for parts in ring]


def trace_id(traceparent: str | None) -> str | None:
    """The trace id of a W3C `traceparent` header, or None when it does not parse."""
    matched = TRACEPARENT.fullmatch((traceparent or "").encode())
    return matched and matched[1].decode()


def _padded_reply(head: str) -> bytes:
    encoded = head.encode()
    return encoded + PADDING[: max(REPLY_BYTES - len(encoded) - 2, 0)] + b'"}'


def tool_reply(id: str, check: int) -> bytes:
    """The tool reply copying `id` and `check`, `REPLY_BYTES` long."""
    return _padded_reply(f'{{"id":"{id}","ok":true,"check":{check},"content":"')


def full_reply(id: str, name: str, trace: str, lang: str, check: int) -> bytes:
    """The `full` reply echoing the body's id, the path's tool name, the trace id, `lang` and `check`."""
    return _padded_reply(f'{{"id":"{id}","name":"{name}","trace":"{trace}","lang":"{lang}","check":{check},"content":"')


def fanout_reply(id: str, width: int) -> bytes:
    """The fan-out reply after `width` matching upstream replies, `REPLY_BYTES` long."""
    return _padded_reply(f'{{"id":"{id}","ok":true,"width":{width},"content":"')


def count_reply(count: int) -> bytes:
    """The context reply: the byte count as 16 lowercase hexadecimal digits."""
    return f"{count:016x}".encode()


def health(framework: str, client: str, batching: bool) -> bytes:
    """The `/health` body naming the pair and whether the server batches."""
    return f'{{"framework":"{framework}","client":"{client}","batching":{str(batching).lower()}}}'.encode()


if __name__ == "__main__":
    first, last = ToolParts(42, 0), ToolParts(42, RING - 1)
    full = (
        first.full_path().encode()
        + first.traceparent().encode()
        + first.full_body()
        + full_reply(first.id, TOOL_NAME, first.trace_id(), LANG, first.check())
    )
    bodies = (
        first.body() + tool_reply(first.id, first.check()),
        last.body() + tool_reply(last.id, last.check()),
        full,
        context_body(42, 256 << 10),
    )
    digests = tuple(hashlib.sha256(body).hexdigest() for body in bodies)
    for name, digest in zip(("tool 0", "tool 1023", "full 0", "context 256KB"), digests):
        print(f"{name}: {digest}")
    assert len(bodies[0]) == TOOL_BYTES + REPLY_BYTES and len(first.full_body()) == TOOL_BYTES
    assert trace_id(first.traceparent()) == first.trace_id()
    assert digests == DIGESTS, "the bodies drifted from payloads.rs"
