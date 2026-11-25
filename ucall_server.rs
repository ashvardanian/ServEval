//! UCall's Rust binding: one server over every interface, whose event loops serve all of them, each polled by its own
//! thread that also drives its loop's bound fan-out client and streamed uploads, and Noise IK or XX for security.

use std::{
    borrow::Cow,
    cell::Cell,
    collections::VecDeque,
    future::Future,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    rc::Rc,
    sync::{Mutex, OnceLock},
    task::{Context, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

use bytes::Bytes;
use ucall::{
    Batch, Batching, BodyDelivery, BytesView, Client, ClientCall, ClientCallOptions, ClientOptions, Completed,
    DetachedRequest, HTTPStatus, Interface, NoiseKey, PeerAuthorizer, Polling, Request, Server, Status, Verb,
};

use serveval::{
    arguments::{self, Security, ServerArgs},
    payloads, stats,
};

const JSON: &str = "content-type: application/json";
const OCTETS: &str = "content-type: application/octet-stream";
const TEXT: &str = "content-type: text/plain";
const IDENTITY: &str = "accept-encoding: identity";
/// The longest one poll waits; an unbounded wait fails the loop with an I/O error on macOS's kqueue backend.
const POLL_SLICE: Duration = Duration::from_millis(100);

/// UCall serving the tool, context and fan-out workloads, and batched tool calls.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    server: ServerArgs,
}

/// The Noise pattern `--security` names; UCall speaks Noise rather than TLS.
fn noise_of(security: Security) -> ucall::Security {
    match security {
        Security::None => ucall::Security::None,
        Security::NoiseXx => ucall::Security::NoiseXX,
        Security::NoiseIk => ucall::Security::NoiseIK,
        Security::Tls => {
            arguments::exit("UCall secures connections with Noise; use --security none, noise-xx or noise-ik")
        }
    }
}

/// The key keys.sh wrote as 64 hexadecimal digits to `<data-dir>/noise/<name>`.
fn read_key(data_dir: &Path, name: &str) -> NoiseKey {
    let path = data_dir.join("noise").join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let bytes: Option<Vec<u8>> = (text.trim().as_bytes().chunks(2))
        .map(|digits| u8::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok())
        .collect();
    bytes.and_then(|bytes| bytes.try_into().ok()).unwrap_or_else(|| {
        arguments::exit(&format!(
            "{} holds no Noise key; run keys.sh with UCall's ucall tool on PATH",
            path.display()
        ))
    })
}

/// The identities of a Noise run: this server's, the client every server admits, and the server key fan-outs expect.
struct Keys {
    server: NoiseKey,
    server_public: NoiseKey,
    client: NoiseKey,
    client_public: NoiseKey,
}

/// A fan-out the polling thread forwards to its upstreams and answers once every reply arrived.
struct Fanout {
    request: Request,
    body: Vec<u8>,
    expected: Bytes,
    reply: Bytes,
    width: usize,
    /// Upstream calls not yet admitted by the client.
    unsent: usize,
    /// Upstream calls not yet finished.
    unfinished: usize,
    failed: bool,
}

/// What a handler hands its own polling thread.
enum Job {
    Upload(DetachedRequest),
    Fanout(Fanout),
}

fn refused<T>(error: ucall::Error) -> T {
    arguments::exit(&format!("UCall refused a route: {error}"))
}

fn reject(request: Request, status: HTTPStatus) {
    let _ = request.reply_error(status, "");
}

fn reply(request: Request, header: &str, body: &[u8]) {
    let _ = request.reply(HTTPStatus::OK, &[BytesView::from(header)], body);
}

fn utf8(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()
}

fn raw(request: Request) {
    let mut head = [0; payloads::REPLY_BYTES];
    let body = request.body();
    let length = body.len().min(head.len());
    head[..length].copy_from_slice(&body[..length]);
    reply(request, OCTETS, &head[..length]);
}

fn tool(request: Request) {
    match serde_json::from_slice::<payloads::ToolCall>(request.body()) {
        Ok(call) => reply(request, JSON, &payloads::tool_reply(&call.id, call.check())),
        Err(_) => reject(request, HTTPStatus::BAD_REQUEST),
    }
}

fn full_reply(request: &Request) -> Result<Bytes, HTTPStatus> {
    if request.header("authorization") != Some(payloads::AUTHORIZATION.as_bytes()) {
        return Err(HTTPStatus::UNAUTHORIZED);
    }
    let traceparent = request.header("traceparent").unwrap_or_default();
    let trace = payloads::trace_id(traceparent).ok_or(HTTPStatus::BAD_REQUEST)?;
    let name = request.path_parameter("name").and_then(utf8);
    let lang = request.query("lang").and_then(utf8);
    let after = request
        .query("after")
        .and_then(utf8)
        .and_then(|after| after.parse::<u64>().ok());
    let call = serde_json::from_slice::<payloads::FullCall>(request.body()).ok();
    let (Some(name), Some(lang), Some(after), Some(call)) = (name, lang, after, call) else {
        return Err(HTTPStatus::BAD_REQUEST);
    };
    Ok(payloads::full_reply(
        &call.id,
        name,
        trace,
        lang,
        after + u64::from(call.top_k),
    ))
}

fn full(request: Request) {
    match full_reply(&request) {
        Ok(body) => reply(request, JSON, &body),
        Err(status) => reject(request, status),
    }
}

fn batch(batch: Batch) {
    batch.into_iter().for_each(tool);
}

/// Counts a streamed body as its chunks arrive, then answers with the count; `pulls` counts the pulls it started.
async fn upload(mut request: DetachedRequest, pulls: Rc<Cell<u64>>) {
    let mut bytes = 0u64;
    loop {
        pulls.set(pulls.get() + 1);
        match request.next_body_chunk().await.map(<[u8]>::len) {
            Ok(0) => break,
            Ok(length) => bytes += length as u64,
            Err(_) => {
                let _ = request.reply_error(HTTPStatus::BAD_REQUEST, "");
                return;
            }
        }
    }
    let _ = request.reply(HTTPStatus::OK, &[TEXT.into()], payloads::count_reply(bytes).as_bytes());
}

/// The upstream `/tool` URLs, the security of every fan-out call, and the connections each loop's client holds.
struct Upstreams {
    urls: Vec<&'static str>,
    options: ClientCallOptions,
    connections: usize,
}

/// One event loop's fan-outs between polls: the slots their calls carry as context, and those with calls to admit.
#[derive(Default)]
struct Fanouts {
    slots: Vec<Option<Fanout>>,
    free: Vec<usize>,
    waiting: VecDeque<usize>,
}

impl Fanouts {
    fn add(&mut self, fanout: Fanout) {
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.slots.push(None);
                self.slots.len() - 1
            }
        };
        self.slots[slot] = Some(fanout);
        self.waiting.push_back(slot);
    }

    /// Answers fan-out `slot` once none of its calls is left unfinished.
    fn settle(&mut self, slot: usize) {
        if self.slots[slot].as_ref().is_some_and(|fanout| fanout.unfinished == 0) {
            let Some(fanout) = self.slots[slot].take() else { return };
            self.free.push(slot);
            match fanout.failed {
                true => reject(fanout.request, HTTPStatus::BAD_GATEWAY),
                false => reply(fanout.request, JSON, &fanout.reply),
            }
        }
    }

    /// Admits the waiting calls in order until the client is full.
    fn submit(&mut self, client: &mut Client, upstreams: &Upstreams) {
        while let Some(&slot) = self.waiting.front() {
            let Some(fanout) = self.slots[slot].as_mut() else {
                return;
            };
            let call = ClientCall {
                verb: Verb::Post,
                url: Cow::Borrowed(upstreams.urls[(fanout.width - fanout.unsent) % upstreams.urls.len()]),
                headers: vec![Cow::Borrowed(JSON.as_bytes()), Cow::Borrowed(IDENTITY.as_bytes())],
                body: Cow::Owned(fanout.body.clone()),
                options: upstreams.options,
            };
            match client.submit(call, slot as u64) {
                Ok(()) => {}
                Err(error) if error.status == Status::Contended => return,
                Err(_) => {
                    fanout.unfinished -= 1;
                    fanout.failed = true;
                }
            }
            fanout.unsent -= 1;
            if fanout.unsent == 0 {
                self.waiting.pop_front();
                self.settle(slot);
            }
        }
    }

    fn finish(&mut self, completed: Completed) {
        let slot = completed.context as usize;
        let Some(fanout) = self.slots[slot].as_mut() else {
            return;
        };
        let expected = completed.response.is_ok_and(|response| {
            response.status_code() == HTTPStatus::OK && response.body().is_ok_and(|body| body == fanout.expected)
        });
        fanout.failed |= !expected;
        fanout.unfinished -= 1;
        self.settle(slot);
    }
}

/// Polls `event_loop` until the server stops, between polls handing the jobs its handlers queued to its bound client
/// and its uploads.
fn poll(server: &Server, event_loop: usize, inbox: &Mutex<Vec<Job>>, client: &mut Client, upstreams: &Upstreams) {
    let mut fanouts = Fanouts::default();
    let mut uploads: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let mut completed: Vec<Completed> = (0..upstreams.connections).map(|_| Completed::default()).collect();
    let mut context = Context::from_waker(Waker::noop());
    let pulls = Rc::new(Cell::new(0));
    let mut pulled = false;
    loop {
        // A pull the loop answers from bytes it already holds finishes inside a poll without waking it, so the poll
        // after new pulls does not wait.
        match server.take_call(event_loop, if pulled { Duration::ZERO } else { POLL_SLICE }) {
            Ok(Polling::Serving) => {}
            Ok(Polling::Retired) => return,
            Err(error) => arguments::exit(&format!("event loop {event_loop} failed: {error}")),
        }
        for job in inbox.lock().expect("no handler panics").drain(..) {
            match job {
                Job::Upload(request) => uploads.push(Box::pin(upload(request, pulls.clone()))),
                Job::Fanout(fanout) => fanouts.add(fanout),
            }
        }
        fanouts.submit(client, upstreams);
        if let Ok(finished) = client.take_completed(&mut completed, Duration::ZERO) {
            finished
                .iter_mut()
                .for_each(|done| fanouts.finish(std::mem::take(done)));
        }
        fanouts.submit(client, upstreams);
        let started = pulls.get();
        uploads.retain_mut(|upload| upload.as_mut().poll(&mut context).is_pending());
        pulled = pulls.get() != started;
    }
}

fn main() {
    let Cli { server: args } = arguments::parse_cli();
    let common = &args.common;
    let security = noise_of(common.security);
    let threads = common.threads.0.get();
    let keys = (security != ucall::Security::None).then(|| Keys {
        server: read_key(&common.data_dir, "server.key"),
        server_public: read_key(&common.data_dir, "server.pub"),
        client: read_key(&common.data_dir, "client.key"),
        client_public: read_key(&common.data_dir, "client.pub"),
    });
    let admit_client = |key: &NoiseKey| keys.as_ref().is_some_and(|keys| key == &keys.client_public);
    let admit_server = |key: &NoiseKey| keys.as_ref().is_some_and(|keys| key == &keys.server_public);
    let scheme = match security {
        ucall::Security::None => "http",
        _ => "http+noise",
    };
    let upstreams = Upstreams {
        urls: (args.upstreams.iter())
            .map(|upstream| &*Box::leak(format!("{scheme}://{upstream}/tool").into_boxed_str()))
            .collect(),
        options: ClientCallOptions {
            security,
            noise_peer_key: keys.as_ref().map(|keys| keys.server_public),
            ..Default::default()
        },
        // Each loop shares the upstream connections among the downstream calls it may hold.
        connections: args.admission().div_ceil(threads) * args.upstreams.len().max(1),
    };
    let owners: Vec<OnceLock<ThreadId>> = (0..threads).map(|_| OnceLock::new()).collect();
    let inboxes: Vec<Mutex<Vec<Job>>> = (0..threads).map(|_| Mutex::default()).collect();
    let hand = |job: Job| {
        let current = thread::current().id();
        let event_loop = owners.iter().position(|owner| owner.get() == Some(&current));
        let inbox = event_loop
            .map(|event_loop| &inboxes[event_loop])
            .expect("handlers run on polling threads");
        inbox.lock().expect("no handler panics").push(job);
    };
    let context = |request: Request| hand(Job::Upload(request.detach()));
    let fanout = |request: Request| {
        let call = serde_json::from_slice::<payloads::ToolCall>(request.body()).ok();
        let width = (request.query("width").and_then(utf8)).and_then(|width| width.parse::<usize>().ok());
        let (Some(call), Some(width @ 1..)) = (call, width) else {
            return reject(request, HTTPStatus::BAD_REQUEST);
        };
        if upstreams.urls.is_empty() {
            return reject(request, HTTPStatus::SERVICE_UNAVAILABLE);
        }
        hand(Job::Fanout(Fanout {
            body: request.body().to_vec(),
            expected: payloads::tool_reply(&call.id, call.check()),
            reply: payloads::fanout_reply(&call.id, width),
            width,
            unsent: width,
            unfinished: width,
            failed: false,
            request,
        }))
    };
    let unit = |request: Request| reply(request, JSON, stats::unit_json().as_bytes());
    let health = |request: Request| reply(request, JSON, payloads::health("ucall", "ucall", true).as_bytes());

    let interfaces: Vec<Interface> = (common.interfaces.iter())
        .map(|interface| {
            let address = SocketAddr::new(interface.addresses[0], common.port);
            let mut listener = Interface::new(address.to_string())
                .event_loops(threads)
                .connections(args.admission());
            if let Some(device) = &interface.device {
                listener = listener.device_name(device);
            }
            if let Some(keys) = &keys {
                listener = listener
                    .security(security, keys.server)
                    .peer_authorizer(PeerAuthorizer::new(&admit_client));
            }
            listener
        })
        .collect();
    let mut server = Server::new(&interfaces).unwrap_or_else(|error| arguments::exit(&format!("UCall: {error}")));
    if server.event_loops() != threads {
        arguments::exit(&format!(
            "UCall runs {} event loops, not {threads}",
            server.event_loops()
        ));
    }
    let batching = Batching {
        min_size: args.batch_size,
        max_size: args.batch_size,
        max_latency: args.batch_wait,
    };
    let routes = [
        server.post("/raw", &raw),
        server.post("/tool", &tool),
        server.post("/tool/{name}", &full),
        server.route_with("/context", Verb::Put, BodyDelivery::Streamed, &context),
        server.post("/fanout", &fanout),
        server.get("/stats", &unit),
        server.get("/health", &health),
    ];
    routes.into_iter().for_each(|route| route.unwrap_or_else(refused));
    let batches = server
        .batch("/tool/batch", Verb::Post, batching, &batch)
        .unwrap_or_else(refused);
    let addresses: Vec<&str> = (0..interfaces.len())
        .filter_map(|index| server.address(index))
        .collect();
    println!("Listening on {} with {threads} UCall event loops", addresses.join(", "));

    let server = &server;
    thread::scope(|scope| {
        for (event_loop, (owner, inbox)) in owners.iter().zip(&inboxes).enumerate() {
            let (admit_server, keys, upstreams) = (&admit_server, &keys, &upstreams);
            scope.spawn(move || {
                let _ = owner.set(thread::current().id());
                let options = ClientOptions {
                    connections: upstreams.connections,
                    noise_private_key: keys.as_ref().map(|keys| keys.client),
                    peer_authorizer: keys.as_ref().map(|_| PeerAuthorizer::new(admit_server)),
                    server: Some(server),
                    event_loop,
                    ..Default::default()
                };
                let mut client =
                    Client::new(options).unwrap_or_else(|error| arguments::exit(&format!("UCall client: {error}")));
                poll(server, event_loop, inbox, &mut client, upstreams);
            });
        }
        scope.spawn(|| batches.take_batches());
        scope.spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
            let runtime = runtime.unwrap_or_else(|error| arguments::exit(&format!("signals: {error}")));
            runtime.block_on(arguments::stop_signal());
            server.stop();
        });
    });
}
