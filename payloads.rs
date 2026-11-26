//! Seeded request bodies and fixed-size replies, byte-identical with `payloads.py`.

use std::io::Write;

use bytes::Bytes;

use crate::arguments::{Parsing, Run, Variant, Workload};

/// Bytes of every tool call body, JSON or protobuf.
pub const TOOL_BYTES: usize = 1024;
/// Bytes of every tool and fan-out reply, JSON or protobuf.
pub const REPLY_BYTES: usize = 256;
/// Distinct tool calls generated at startup and cycled through.
pub const RING: usize = 1024;
/// `ToolCall.trace` characters that pad the protobuf message to `TOOL_BYTES`.
pub const MESSAGE_TRACE_BYTES: usize = 888;
/// `ToolReply.content` bytes that pad the protobuf reply without a trace id to `REPLY_BYTES`.
pub const MESSAGE_CONTENT_BYTES: usize = 222;
/// The largest message a gRPC receiver takes by default: gRPC core's `GRPC_DEFAULT_MAX_RECV_MESSAGE_LENGTH` and
/// tonic's `DEFAULT_MAX_RECV_MESSAGE_SIZE`, both 4MB.
pub const GRPC_DEFAULT_MAXIMUM_RECEIVE_MESSAGE_BYTES: usize = 4 << 20;
/// Bytes of each `Chunk` a gRPC upload or download streams: the default receive limit less the `data` field's own
/// framing, its one-byte tag and the four-byte varint its length takes from 2^21 up to 2^28, so each message is as
/// large as every default receiver takes and a body crosses in as few messages as the protocol allows.
pub const CHUNK_BYTES: usize = GRPC_DEFAULT_MAXIMUM_RECEIVE_MESSAGE_BYTES - 1 - 4;
pub const JSON: &str = "application/json";
pub const OCTETS: &str = "application/octet-stream";
/// The tool every call names, in the body and in the `full` path.
pub const TOOL_NAME: &str = "search_documents";
pub const LANG: &str = "en";
/// The `authorization` header of every `full` call, which servers answer with 401 when it differs.
pub const AUTHORIZATION: &str = "Bearer 6f1c09a4e2b7d35c81f0aa4e92d1c7b3";
/// The metadata entry of a gRPC `Health` reply carrying `max_concurrent_streams_per_connection`, which `HealthReply`
/// has no field for; a reply without it advertises no limit.
pub const MAX_CONCURRENT_STREAMS_METADATA: &str = "max-concurrent-streams-per-connection";

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
const QUERY_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz      ";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
const QUERY_BYTES: usize = 64;
/// 2026-01-01 in Unix seconds, the earliest `after`, which then spans a year.
const AFTER_FIRST: u64 = 1_767_225_600;
const AFTER_SPAN: u64 = 365 * 86_400;
static PADDING: [u8; REPLY_BYTES] = [b'x'; REPLY_BYTES];

/// SHA-256 of tool call 0 and 1023 with their replies, of the `full` call 0 with its reply, and of a 256KB context
/// body, for seed 42, pinned in `payloads.py` too.
pub const DIGESTS: [&str; 4] = [
    "69cb58f431dcf185b8fbea14a165f28edc5f80cfbb6f342501bce01d21f45286",
    "671c52debf7b9527575511306acc935b86ffce8f6b3549f5ef38d993d00e9966",
    "9179f5c8c682f517a676366d288fb66636ecd77fddbc1a93acc22764b3431aa1",
    "121da22f71b9cc6e4f5cd9a8654c7767e8513f3e7b40e6ec8f22f8b2c00a72e0",
];

/// SplitMix64's finalizer.
pub fn split_mix_bits(value: u64) -> u64 {
    let value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// The `index`-th stream of the family `name` under `seed`, as UCall's tools derive it.
pub fn stream_key(seed: u64, name: &str, index: u64) -> u64 {
    let hashed = name.bytes().fold(0xCBF2_9CE4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01B3)
    });
    split_mix_bits(split_mix_bits(seed ^ hashed).wrapping_add(index))
}

/// SplitMix64 over one counter, emitting each draw as 8 little-endian bytes.
pub struct SplitMix(pub u64);

impl SplitMix {
    pub fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(GOLDEN_GAMMA);
        split_mix_bits(self.0)
    }

    pub fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&self.draw().to_le_bytes()[..chunk.len()]);
        }
    }
}

/// The seeded fields of one tool call, shared by its JSON bodies and its protobuf message.
pub struct ToolParts {
    pub id: String,
    pub query: String,
    pub top_k: u32,
    /// Unix seconds.
    pub after: u64,
    /// Hexadecimal padding, longer than any encoding needs, ending in the span and trace ids no body reaches.
    pub trace: String,
}

impl ToolParts {
    pub fn new(seed: u64, index: usize) -> Self {
        let mut generator = SplitMix(stream_key(seed, "tool", index as u64));
        let id = format!("call_{:016x}", generator.draw());
        let numbers = generator.draw();
        let mut bytes = [0u8; TOOL_BYTES];
        generator.fill(&mut bytes);
        let (query, trace) = bytes.split_at(QUERY_BYTES);
        Self {
            id,
            query: query
                .iter()
                .map(|&byte| char::from(QUERY_ALPHABET[usize::from(byte & 31)]))
                .collect(),
            top_k: 1 + (numbers % 64) as u32,
            after: AFTER_FIRST + (numbers >> 32) % AFTER_SPAN,
            trace: trace
                .iter()
                .map(|&byte| char::from(HEX_DIGITS[usize::from(byte & 15)]))
                .collect(),
        }
    }

    /// What every reply echoes to prove the server read both typed values.
    pub fn check(&self) -> u64 {
        self.after + u64::from(self.top_k)
    }

    pub fn trace_id(&self) -> &str {
        &self.trace[self.trace.len() - 32..]
    }

    pub fn traceparent(&self) -> String {
        let span = &self.trace[self.trace.len() - 48..self.trace.len() - 32];
        format!("00-{}-{span}-01", self.trace_id())
    }

    fn padded(&self, head: String) -> Bytes {
        let trace = &self.trace[..TOOL_BYTES - head.len() - 2];
        Bytes::from(format!("{head}{trace}\"}}"))
    }

    /// The JSON body, exactly `TOOL_BYTES` long.
    pub fn body(&self) -> Bytes {
        self.padded(format!(
            r#"{{"id":"{}","type":"function","name":"{TOOL_NAME}","arguments":{{"query":"{}","top_k":{},"filters":{{"lang":"{LANG}","after":{}}}}},"trace":""#,
            self.id, self.query, self.top_k, self.after
        ))
    }

    /// The `full` call's JSON body, exactly `TOOL_BYTES` long, its name, `lang` and `after` moved to the URL.
    pub fn full_body(&self) -> Bytes {
        self.padded(format!(
            r#"{{"id":"{}","query":"{}","top_k":{},"trace":""#,
            self.id, self.query, self.top_k
        ))
    }

    pub fn full_path(&self) -> String {
        format!("/tool/{TOOL_NAME}?lang={LANG}&after={}", self.after)
    }
}

/// The `RING` tool calls of `seed`.
pub fn tool_ring(seed: u64) -> Vec<ToolParts> {
    (0..RING).map(|index| ToolParts::new(seed, index)).collect()
}

/// A context upload of `bytes` seeded bytes.
pub fn context_body(seed: u64, bytes: u64) -> Bytes {
    let mut body = vec![0u8; bytes as usize];
    SplitMix(stream_key(seed, "context", 0)).fill(&mut body);
    Bytes::from(body)
}

/// The path of a download of `bytes` bytes.
pub fn download_path(bytes: u64) -> String {
    format!("/context?bytes={bytes}")
}

/// The first `bytes` bytes of the prepared download body, `None` when it holds fewer.
pub fn download(prepared: &Bytes, bytes: u64) -> Option<Bytes> {
    let bytes = usize::try_from(bytes).ok().filter(|&bytes| bytes <= prepared.len())?;
    Some(prepared.slice(..bytes))
}

/// Compares a download's reply chunk by chunk with the bytes it must equal, so a client checks the length and every
/// byte without gathering the reply first.
pub struct DownloadCheck<'expected> {
    expected: &'expected [u8],
    received: usize,
    matching: bool,
}

impl<'expected> DownloadCheck<'expected> {
    pub fn new(expected: &'expected [u8]) -> Self {
        Self {
            expected,
            received: 0,
            matching: true,
        }
    }

    /// Compares the next `chunk` of the reply with the expected bytes at its offset.
    pub fn chunk(&mut self, chunk: &[u8]) {
        let rest = self.expected.get(self.received..).unwrap_or_default();
        self.matching &= rest.starts_with(chunk);
        self.received += chunk.len();
    }

    /// The bytes received when every one matched and none was missing, `None` otherwise.
    pub fn matched(&self) -> Option<u64> {
        (self.matching && self.received == self.expected.len()).then_some(self.received as u64)
    }
}

/// One HTTP tool or fan-out call as a client sends it, and the reply it must get back byte for byte.
pub struct Exchange {
    pub path: String,
    /// The `traceparent` header of a `full` call, sent with `AUTHORIZATION`.
    pub traceparent: Option<String>,
    pub body: Bytes,
    pub reply: Bytes,
}

/// The calls of a tool or fan-out `run`, one per ring entry.
pub fn exchanges(ring: &[ToolParts], run: &Run, width: usize) -> Vec<Exchange> {
    let batch = match run.variant {
        Variant::Plain => "",
        Variant::Batched => "/batch",
    };
    let exchange = |parts: &ToolParts| {
        let (path, traceparent, body, reply) = match (run.workload, run.parsing) {
            (Workload::Fanout, _) => (
                format!("/fanout{batch}?width={width}"),
                None,
                parts.body(),
                fanout_reply(&parts.id, width),
            ),
            (_, Parsing::Raw) => {
                let body = parts.body();
                ("/raw".into(), None, body.clone(), body.slice(..REPLY_BYTES))
            }
            (_, Parsing::Body) => (
                format!("/tool{batch}"),
                None,
                parts.body(),
                tool_reply(&parts.id, parts.check()),
            ),
            (_, Parsing::Full) => (
                parts.full_path(),
                Some(parts.traceparent()),
                parts.full_body(),
                full_reply(&parts.id, TOOL_NAME, parts.trace_id(), LANG, parts.check()),
            ),
        };
        Exchange {
            path,
            traceparent,
            body,
            reply,
        }
    };
    ring.iter().map(exchange).collect()
}

/// A tool call as the HTTP servers parse it.
#[derive(serde::Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub arguments: ToolArguments,
    pub trace: String,
}

#[derive(serde::Deserialize)]
pub struct ToolArguments {
    pub query: String,
    pub top_k: u32,
    pub filters: ToolFilters,
}

#[derive(serde::Deserialize)]
pub struct ToolFilters {
    pub lang: String,
    pub after: u64,
}

impl ToolCall {
    pub fn check(&self) -> u64 {
        self.arguments.filters.after + u64::from(self.arguments.top_k)
    }
}

/// The body of a `full` call as the HTTP servers parse it.
#[derive(serde::Deserialize)]
pub struct FullCall {
    pub id: String,
    pub query: String,
    pub top_k: u32,
    pub trace: String,
}

/// The query of a download, `GET /context?bytes=N`.
#[derive(serde::Deserialize)]
pub struct DownloadQuery {
    pub bytes: u64,
}

/// The query of a `full` call.
#[derive(serde::Deserialize)]
pub struct FullQuery {
    pub lang: String,
    pub after: u64,
}

/// The trace id of a W3C `traceparent` header, or `None` when it does not parse.
pub fn trace_id(traceparent: &[u8]) -> Option<&str> {
    let well_formed = traceparent.len() == 55
        && traceparent.iter().enumerate().all(|(index, &byte)| match index {
            2 | 35 | 52 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        });
    well_formed
        .then(|| std::str::from_utf8(&traceparent[3..35]).ok())
        .flatten()
}

fn padded_reply(head: std::fmt::Arguments) -> Bytes {
    let mut reply = Vec::with_capacity(REPLY_BYTES);
    reply.write_fmt(head).expect("a vector grows");
    let padding = REPLY_BYTES.saturating_sub(reply.len() + 2);
    reply.extend_from_slice(&PADDING[..padding]);
    reply.extend_from_slice(b"\"}");
    Bytes::from(reply)
}

/// The tool reply copying `id` and `check`, `REPLY_BYTES` long.
pub fn tool_reply(id: &str, check: u64) -> Bytes {
    padded_reply(format_args!(r#"{{"id":"{id}","ok":true,"check":{check},"content":""#))
}

/// The `full` reply echoing the body's id, the path's tool name, the trace id, `lang` and `check`.
pub fn full_reply(id: &str, name: &str, trace: &str, lang: &str, check: u64) -> Bytes {
    padded_reply(format_args!(
        r#"{{"id":"{id}","name":"{name}","trace":"{trace}","lang":"{lang}","check":{check},"content":""#
    ))
}

/// The fan-out reply after `width` matching upstream replies, `REPLY_BYTES` long.
pub fn fanout_reply(id: &str, width: usize) -> Bytes {
    padded_reply(format_args!(r#"{{"id":"{id}","ok":true,"width":{width},"content":""#))
}

/// The context reply: the byte count as 16 lowercase hexadecimal digits.
pub fn count_reply(bytes: u64) -> String {
    format!("{bytes:016x}")
}

/// The `/health` body naming the pair, whether the server batches and its `--batch-size`, null when it does not. Its
/// `max_concurrent_streams_per_connection`, the `SETTINGS_MAX_CONCURRENT_STREAMS` a server advertises, is null: its
/// clients drive every Rust server answering in JSON over HTTP/1.1, and tonic answers `Health` in protobuf, under
/// `MAX_CONCURRENT_STREAMS_METADATA`.
pub fn health(framework: &str, client: &str, batch_size: Option<usize>) -> String {
    let batching = batch_size.is_some();
    let batch_size = batch_size.map_or_else(|| "null".to_owned(), |size| size.to_string());
    format!(
        r#"{{"framework":"{framework}","client":"{client}","batching":{batching},"batch_size":{batch_size},"max_concurrent_streams_per_connection":null}}"#
    )
}

/// `health` for a server that pipelines HTTP/1.1 requests, adding the `max_pipelined_requests_per_connection` one
/// connection may have in flight, which a client's `server-maximum` reads before the stream limit.
pub fn pipelined_health(
    framework: &str,
    client: &str,
    batch_size: Option<usize>,
    max_pipelined_requests_per_connection: usize,
) -> String {
    let mut body = health(framework, client, batch_size);
    body.pop(); // the closing brace
    format!("{body},\"max_pipelined_requests_per_connection\":{max_pipelined_requests_per_connection}}}")
}

#[cfg(feature = "tonic")]
impl ToolParts {
    /// The protobuf twin of `body`, padded to `TOOL_BYTES`.
    pub fn message(&self) -> crate::proto::ToolCall {
        use crate::proto::{Arguments, Filters, ToolCall};

        ToolCall {
            id: self.id.clone(),
            r#type: "function".into(),
            name: TOOL_NAME.into(),
            arguments: Some(Arguments {
                query: self.query.clone(),
                top_k: self.top_k,
                filters: Some(Filters {
                    lang: LANG.into(),
                    after: self.after,
                }),
            }),
            trace: self.trace[..MESSAGE_TRACE_BYTES].into(),
        }
    }
}

#[cfg(feature = "tonic")]
impl crate::proto::ToolCall {
    /// `after + top_k`, with an absent field read as zero, as protobuf reads it.
    pub fn check(&self) -> u64 {
        let arguments = self.arguments.as_ref();
        let filters = arguments.and_then(|arguments| arguments.filters.as_ref());
        filters.map_or(0, |filters| filters.after) + u64::from(arguments.map_or(0, |arguments| arguments.top_k))
    }
}

/// The protobuf reply copying `id`, `check` and a `full` call's trace id, padded to `REPLY_BYTES`.
#[cfg(feature = "tonic")]
pub fn reply_message(id: &str, check: u64, trace: &str) -> crate::proto::ToolReply {
    let padding = match trace.len() {
        0 => MESSAGE_CONTENT_BYTES,
        length => MESSAGE_CONTENT_BYTES - length - 2,
    };
    crate::proto::ToolReply {
        id: id.into(),
        ok: true,
        content: Bytes::from_static(&PADDING[..padding]),
        check,
        trace: trace.into(),
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn digests() {
        let hex = |bytes: &[u8]| {
            Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let tool = |parts: &ToolParts| [parts.body(), tool_reply(&parts.id, parts.check())].concat();
        let (first, last) = (ToolParts::new(42, 0), ToolParts::new(42, RING - 1));
        let full = [
            Bytes::from(first.full_path()),
            Bytes::from(first.traceparent()),
            first.full_body(),
            full_reply(&first.id, TOOL_NAME, first.trace_id(), LANG, first.check()),
        ]
        .concat();
        let bodies = [tool(&first), tool(&last), full, context_body(42, 256 << 10).to_vec()];
        let digests = bodies.each_ref().map(|body| hex(body));
        for (name, digest) in ["tool 0", "tool 1023", "full 0", "context 256KB"].iter().zip(&digests) {
            println!("{name}: {digest}");
        }
        assert_eq!(digests, DIGESTS);
        assert_eq!(bodies[0].len(), TOOL_BYTES + REPLY_BYTES);
        assert_eq!(first.full_body().len(), TOOL_BYTES);
        assert_eq!(trace_id(first.traceparent().as_bytes()), Some(first.trace_id()));
    }

    #[test]
    #[cfg(feature = "tonic")]
    fn chunk_fills_the_default_receive_limit() {
        use prost::Message;

        let chunk = crate::proto::Chunk {
            data: Bytes::from(vec![0; CHUNK_BYTES]),
        };
        assert_eq!(chunk.encoded_len(), GRPC_DEFAULT_MAXIMUM_RECEIVE_MESSAGE_BYTES);
    }
}
