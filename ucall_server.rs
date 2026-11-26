//! UCall's Rust binding: one server over every interface, whose event loops serve all of them, each polled by its own
//! thread that also drives its loop's bound fan-out client and streamed uploads, and Noise IK or XX for security.
//!
//! UCall starts no threads, so this program places its own: one ForkUnion pool per memory domain that hosts an
//! interface, pinned to the cores of the affinity mask local to that domain, each thread polling one loop. The loops
//! number those cores, whatever `--threads` says. Those polling threads are the only ones serving: between polls each
//! claims every `/tool/batch` endpoint without waiting and answers whatever batch is due, so no thread waits on a batch.

use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::BTreeMap,
    collections::VecDeque,
    future::Future,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicPtr, Ordering},
        OnceLock,
    },
    task::{Context, Waker},
    time::Duration,
};

use bytes::Bytes;
use forkunion::{CallerExclusivity, ComputeDomain, ThreadPool, Topology};
use ucall::{
    Batch, BatchEndpoint, Batching, BodyDelivery, BytesView, Client, ClientCall, ClientCallOptions, ClientOptions,
    Completed, Deferred, DetachedRequest, EventLoopStatistics, HTTPStatus, Interface, NoiseKey, PeerAuthorizer,
    Polling, ReplyPart, Request, Server, Status, Verb,
};

use serveval::{
    arguments::{self, DownloadBody, Security, ServerArgs},
    payloads, stats,
};

const JSON: &str = "content-type: application/json";
const OCTETS: &str = "content-type: application/octet-stream";
const TEXT: &str = "content-type: text/plain";
const IDENTITY: &str = "accept-encoding: identity";
/// RFC 9110's 416 Range Not Satisfiable, which UCall names no constant for.
const RANGE_NOT_SATISFIABLE: HTTPStatus = HTTPStatus(416);
/// The longest one poll waits; an unbounded wait fails the loop with an I/O error on macOS's kqueue backend.
const POLL_SLICE: Duration = Duration::from_millis(100);
/// Requests one connection may have in flight: RFC 9113's recommended minimum of concurrent streams, the depth tonic
/// and grpc.aio allow when they advertise no limit, so every stack's throughput test runs at one depth.
const MAX_PIPELINED_REQUESTS: usize = arguments::RFC_9113_MINIMUM_CONCURRENT_STREAMS;

/// UCall serving the tool, context, download and fan-out workloads, and batched tool calls.
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

thread_local! {
    /// The jobs this polling thread's handlers queued since its last poll: UCall runs a loop's handlers on the thread
    /// polling it, so each loop's inbox is its thread's own, with no lock and no lookup of which loop it is.
    static INBOX: RefCell<Vec<Job>> = const { RefCell::new(Vec::new()) };

    /// This polling thread's download reply slots: a reply sending the prepared body in place stays deferred until
    /// the kernel let go of it, and only this thread finishes it, so it never waits on one.
    static DOWNLOADS: RefCell<DownloadSlots> = RefCell::new(DownloadSlots::default());
}

/// One polling thread's download reply slots: those whose replies may still send, oldest first, as a loop finishes
/// its replies roughly in order, and those free again, so each download takes a slot in amortized constant time.
#[derive(Default)]
struct DownloadSlots {
    slots: Vec<Pin<Box<Deferred<'static>>>>,
    sending: VecDeque<usize>,
    free: Vec<usize>,
}

impl DownloadSlots {
    /// A slot whose previous reply is final: the oldest sending ones that turned final return first, else a new one.
    fn take(&mut self) -> usize {
        while let Some(&oldest) = self.sending.front() {
            if self.slots[oldest].status() == Err(Status::Deferred) {
                break;
            }
            self.sending.pop_front();
            self.free.push(oldest);
        }
        let index = self.free.pop().unwrap_or_else(|| {
            self.slots.push(Box::pin(Deferred::new()));
            self.slots.len() - 1
        });
        self.sending.push_back(index);
        index
    }
}

/// The prepared download body, kept for the whole process, as the replies sending it in place may outlive any scope.
static DOWNLOAD_BODY: OnceLock<Option<&'static [u8]>> = OnceLock::new();

/// The counters of `statistics` a row prices per request, by their UCall names.
fn counters(statistics: &EventLoopStatistics) -> [(&'static str, u64); 16] {
    [
        ("polls", statistics.polls),
        ("empty_polls", statistics.empty_polls),
        ("kernel_enters", statistics.kernel_enters),
        ("kernel_entries", statistics.kernel_entries),
        ("submissions", statistics.submissions),
        ("completions", statistics.completions),
        ("requests", statistics.requests),
        ("accepts", statistics.accepts),
        ("receive_rearms", statistics.receive_rearms),
        ("bytes_received", statistics.bytes_received),
        ("bytes_sent", statistics.bytes_sent),
        ("bytes_copied", statistics.bytes_copied),
        ("zero_copy_sends", statistics.zero_copy_sends),
        ("copying_sends", statistics.copying_sends),
        ("ready_passes", statistics.ready_passes),
        ("replied_dequeues", statistics.replied_dequeues),
    ]
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

/// Replies with the first `bytes` bytes of the prepared download body, which UCall sends in place as a memory part,
/// without copying it; 416 when the server prepared fewer, 500 when it could not prepare any.
fn download(request: Request, prepared: &DownloadBody) {
    let bytes = request
        .query("bytes")
        .and_then(utf8)
        .and_then(|bytes| bytes.parse::<usize>().ok());
    let body = DOWNLOAD_BODY.get_or_init(|| prepared.get().map(|body| &**Box::leak(Box::new(body.clone()))));
    let Some(body) = *body else {
        return reject(request, HTTPStatus::INTERNAL_ERROR);
    };
    let bytes = match bytes {
        None => return reject(request, HTTPStatus::BAD_REQUEST),
        Some(bytes) if bytes > body.len() => return reject(request, RANGE_NOT_SATISFIABLE),
        Some(bytes) => bytes,
    };
    // The body lives as long as the process, and each slot until its thread ends, after its loop made it final.
    let parts = [unsafe { ReplyPart::memory(&body[..bytes]) }];
    DOWNLOADS.with_borrow_mut(|downloads| {
        let index = downloads.take();
        let _ = request.detach().reply_parts(
            HTTPStatus::OK,
            &[OCTETS.into()],
            &parts,
            downloads.slots[index].as_mut(),
        );
    });
}

/// The upstream `/tool` URLs, the security of every fan-out call, and the connections each loop's client holds, none
/// without upstreams.
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

/// Polls `event_loop` until the server stops, between polls answering any due batch of the endpoints no other polling
/// thread holds, then handing the jobs its handlers queued to its bound client and its uploads; without upstreams
/// there is no client, and no fan-out is queued.
///
/// Every polling thread claims the batch endpoints in turn, so a batch fires on the first one to look once it is due:
/// on the thread whose deposit filled it, or for a partial batch on the first poll to end after its oldest request
/// waited `batch_wait`. Polls wait at most `batch_wait`, so that one ends within `batch_wait` of the deadline.
fn poll(
    server: &Server,
    event_loop: usize,
    mut client: Option<&mut Client>,
    upstreams: &Upstreams,
    batches: &[BatchEndpoint],
    poll_slice: Duration,
) {
    let mut fanouts = Fanouts::default();
    let mut uploads: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let mut completed: Vec<Completed> = (0..upstreams.connections).map(|_| Completed::default()).collect();
    let mut context = Context::from_waker(Waker::noop());
    let pulls = Rc::new(Cell::new(0));
    let mut pulled = false;
    loop {
        // A pull the loop answers from bytes it already holds finishes inside a poll without waking it, so the poll
        // after new pulls does not wait.
        match server.take_call(event_loop, if pulled { Duration::ZERO } else { poll_slice }) {
            Ok(Polling::Serving) => {}
            Ok(Polling::Retired) => return,
            Err(error) => arguments::exit(&format!("event loop {event_loop} failed: {error}")),
        }
        for endpoint in batches {
            // A zero timeout never sleeps: it claims the endpoint, fires what is due, and releases it.
            match endpoint.take_batch(Duration::ZERO) {
                Err(error) if error.status != Status::Contended => {
                    arguments::exit(&format!("batch endpoint failed: {error}"))
                }
                _ => {} // answered what was due, or another polling thread holds the endpoint now
            }
        }
        INBOX.with_borrow_mut(|inbox| {
            for job in inbox.drain(..) {
                match job {
                    Job::Upload(request) => uploads.push(Box::pin(upload(request, pulls.clone()))),
                    Job::Fanout(fanout) => fanouts.add(fanout),
                }
            }
        });
        if let Some(client) = &mut client {
            fanouts.submit(client, upstreams);
            if let Ok(finished) = client.take_completed(&mut completed, Duration::ZERO) {
                finished
                    .iter_mut()
                    .for_each(|done| fanouts.finish(std::mem::take(done)));
            }
            fanouts.submit(client, upstreams);
        }
        let started = pulls.get();
        uploads.retain_mut(|upload| upload.as_mut().poll(&mut context).is_pending());
        pulled = pulls.get() != started;
    }
}

/// The cores of `memory_domain` this process may run on, from the kernel's node list and the unit's mask.
fn cores_near(memory_domain: i32, allowed: &[usize]) -> Vec<usize> {
    let node = std::fs::read_to_string(format!("/sys/devices/system/node/node{memory_domain}/cpulist"));
    let node = node.map(|text| stats::parse_cpus(&text)).unwrap_or_default();
    node.into_iter().filter(|core| allowed.contains(core)).collect()
}

/// The memory domain UCall reports for each interface's device, from a throwaway server on port 0: a server fixes its
/// loops at construction, and the loops follow from the domains.
fn memory_domains(interfaces: &[arguments::Interface]) -> Vec<i32> {
    let probes: Vec<Interface> = (interfaces.iter())
        .map(|interface| {
            let probe = Interface::new(SocketAddr::new(interface.addresses[0], 0).to_string())
                .event_loops(1)
                .connections(1);
            match &interface.device {
                Some(device) => probe.device_name(device),
                None => probe,
            }
        })
        .collect();
    let probe = Server::new(&probes).unwrap_or_else(|error| arguments::exit(&format!("UCall: {error}")));
    (0..interfaces.len())
        .map(|index| probe.memory_domain(index).unwrap_or(-1))
        .collect()
}

/// One memory domain's share of the event loops: the cores of the mask local to it, polling loops from `first_loop`.
struct Placement {
    memory_domain: i32,
    cores: Vec<usize>,
    first_loop: usize,
}

/// Each memory domain hosting an interface, in the order the interfaces name them, with the cores of the mask local to
/// it; interfaces on one domain share its cores. Refuses an interface with no domain or no allowed core near it, since
/// polling it from another domain is the cost this placement exists to avoid.
fn placements(interfaces: &[arguments::Interface], memory_domains: &[i32], allowed: &[usize]) -> Vec<Placement> {
    let mut placements: Vec<Placement> = Vec::new();
    for (interface, &memory_domain) in interfaces.iter().zip(memory_domains) {
        if memory_domain < 0 {
            arguments::exit(&format!(
                "UCall reports no memory domain for {interface}; name its device as DEVICE=ADDRESS"
            ));
        }
        if placements
            .iter()
            .any(|placement| placement.memory_domain == memory_domain)
        {
            continue;
        }
        let cores = cores_near(memory_domain, allowed);
        if cores.is_empty() {
            arguments::exit(&format!(
                "no core of the mask {} is local to memory domain {memory_domain} of {interface}",
                stats::spell_cpus(allowed.to_vec())
            ));
        }
        let first_loop = placements.iter().map(|placement| placement.cores.len()).sum();
        placements.push(Placement {
            memory_domain,
            cores,
            first_loop,
        });
    }
    placements
}

/// The one compute domain whose local memory is `memory_domain`, or none when there are several or zero, since the
/// `forkunion` crate spawns on one compute domain and has no spawn near a memory domain.
fn compute_domain_near(topology: &Topology, memory_domain: i32) -> Option<usize> {
    let local = |index: usize| {
        let memory = topology.local_memory_of(ComputeDomain(index));
        let id = memory.and_then(|memory| topology.memory_domain_id_at_index(memory));
        id.is_ok_and(|id| id.get() == memory_domain)
    };
    let mut near = (0..topology.compute_domains_count().ok()?).filter(|&index| local(index));
    let first = near.next()?;
    near.next().is_none().then_some(first)
}

/// Starts every pool's threads, thread `t` of a pool polling loop `first_loop + t`, runs `until_stopped` on the calling
/// thread, then joins the pools innermost first.
fn serve(pollers: &mut [(ThreadPool, usize)], poll: &(dyn Fn(usize) + Sync), until_stopped: &dyn Fn()) {
    let Some(((pool, first_loop), rest)) = pollers.split_first_mut() else {
        return until_stopped();
    };
    let first_loop = *first_loop;
    pool.for_threads(
        |thread, _| poll(first_loop + thread),
        |_| serve(rest, poll, until_stopped),
    );
}

fn main() {
    let Cli { server: args } = arguments::parse_cli();
    let common = &args.common;
    let security = noise_of(common.security);
    // The main thread's mask is the unit's, as systemd's `AllowedCPUs` sets it; only the polling threads narrow theirs.
    let allowed = stats::allowed_cpus();
    let memory_domains = memory_domains(&common.interfaces);
    let placements = placements(&common.interfaces, &memory_domains, &allowed);
    let event_loops: usize = placements.iter().map(|placement| placement.cores.len()).sum();
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
        // Each loop shares the upstream connections among the downstream calls it may hold. Without upstreams no loop
        // makes a client, whose budget would otherwise grow every loop's queue by connections it never opens.
        connections: args.admission().div_ceil(event_loops) * args.upstreams.len(),
    };
    let hand = |job: Job| INBOX.with_borrow_mut(|inbox| inbox.push(job));
    // The server is made after its handlers and borrows them, so `/stats` finds it through this address, set once
    // it exists and before any poll.
    let serving: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
    let context = |request: Request| hand(Job::Upload(request.detach()));
    let prepared = args.download_body();
    let download = |request: Request| download(request, &prepared);
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
    let cpu_list = stats::spell_cpus(allowed.clone());
    let unit = |request: Request| {
        // A polling thread's own mask is its one core, so the reply names the process's mask, read before any pinning.
        let mut event_loop_counters = BTreeMap::new();
        let server = serving.load(Ordering::Acquire).cast_const().cast::<Server>();
        // SAFETY: handlers run only inside polls, which all return before `server` is dropped at `main`'s end.
        if let Some(server) = unsafe { server.as_ref() } {
            let all = (0..server.event_loops()).filter_map(|event_loop| server.event_loop_statistics(event_loop).ok());
            for (name, value) in all.flat_map(|statistics| counters(&statistics)) {
                *event_loop_counters.entry(name.to_owned()).or_insert(0) += value;
            }
        }
        let unit = stats::UnitStats {
            cores: allowed.len() as u64,
            cpu_list: cpu_list.clone(),
            event_loop_counters: Some(event_loop_counters),
            ..stats::unit_stats()
        };
        reply(
            request,
            JSON,
            serde_json::to_string(&unit).unwrap_or_default().as_bytes(),
        )
    };
    let health = payloads::pipelined_health("ucall", "ucall", Some(args.batch_size), MAX_PIPELINED_REQUESTS);
    let health = |request: Request| reply(request, JSON, health.as_bytes());

    let interfaces: Vec<Interface> = (common.interfaces.iter())
        .map(|interface| {
            let address = SocketAddr::new(interface.addresses[0], common.port);
            let mut listener = Interface::new(address.to_string())
                .event_loops(event_loops)
                .connections(args.admission())
                .pipelined_requests(MAX_PIPELINED_REQUESTS);
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
    let topology = Topology::new().unwrap_or_else(|error| arguments::exit(&format!("ForkUnion topology: {error}")));
    // The polling thread that claims a due batch answers it itself: a tool reply costs a few microseconds, and waking
    // a pool for 32 of them cost more than it saved (35-121 µs/call against 8 µs unbatched in the smoke phase).
    let batch = |batch: Batch| batch.into_iter().for_each(tool);
    let mut server = Server::new(&interfaces).unwrap_or_else(|error| arguments::exit(&format!("UCall: {error}")));
    if server.event_loops() != event_loops {
        arguments::exit(&format!(
            "UCall runs {} event loops, not {event_loops}",
            server.event_loops()
        ));
    }
    let batching = Batching {
        min_size: args.batch_size,
        max_size: args.batch_size,
        max_latency: args.batch_wait,
    };
    // Tool calls answer PUT as well as POST: UCall's client pipelines only idempotent calls, so deeper clients PUT.
    let routes = [
        server.post("/raw", &raw),
        server.put("/raw", &raw),
        server.post("/tool", &tool),
        server.put("/tool", &tool),
        server.post("/tool/{name}", &full),
        server.put("/tool/{name}", &full),
        server.route_with("/context", Verb::Put, BodyDelivery::Streamed, &context),
        server.get("/context", &download),
        server.post("/fanout", &fanout),
        server.get("/stats", &unit),
        server.get("/health", &health),
    ];
    routes.into_iter().for_each(|route| route.unwrap_or_else(refused));
    let batches = [Verb::Post, Verb::Put].map(|verb| {
        server
            .batch("/tool/batch", verb, batching, &batch)
            .unwrap_or_else(refused)
    });
    let addresses: Vec<&str> = (0..interfaces.len())
        .filter_map(|index| server.address(index))
        .collect();

    // One exclusive pool per memory domain, its threads pinned to that domain's cores, so each loop's polls, client and
    // uploads run beside the interface's device.
    let mut pollers: Vec<(ThreadPool, usize)> = Vec::new();
    for placement in &placements {
        let Some(compute_domain) = compute_domain_near(&topology, placement.memory_domain) else {
            arguments::exit(&format!(
                "ForkUnion finds no single compute domain local to memory domain {}",
                placement.memory_domain
            ));
        };
        let pool = ThreadPool::spawn_on(
            &topology,
            compute_domain,
            placement.cores.len(),
            CallerExclusivity::Exclusive,
        );
        let pool = pool.unwrap_or_else(|error| arguments::exit(&format!("ForkUnion pool: {error}")));
        pollers.push((pool, placement.first_loop));
    }
    drop(topology); // pools never retain the topology
    for (index, interface) in common.interfaces.iter().enumerate() {
        let placement = (placements.iter())
            .find(|placement| placement.memory_domain == memory_domains[index])
            .expect("every interface has a placement");
        let loops = placement.first_loop..placement.first_loop + placement.cores.len();
        println!(
            "Interface {interface} at {}: memory domain {}, cores {}, event loops {}-{} of {event_loops}",
            addresses.get(index).unwrap_or(&""),
            placement.memory_domain,
            stats::spell_cpus(placement.cores.clone()),
            loops.start,
            loops.end - 1
        );
    }
    println!(
        "Listening on {} with {event_loops} UCall event loops",
        addresses.join(", ")
    );

    let server = &server;
    let poll_slice = POLL_SLICE.min(args.batch_wait);
    serving.store(std::ptr::from_ref(server).cast_mut().cast(), Ordering::Release);
    let poll_loop = |event_loop: usize| {
        // The client is made on its polling thread, so its buffers land on that thread's memory domain.
        let mut client = (!upstreams.urls.is_empty()).then(|| {
            let options = ClientOptions {
                connections: upstreams.connections,
                noise_private_key: keys.as_ref().map(|keys| keys.client),
                peer_authorizer: keys.as_ref().map(|_| PeerAuthorizer::new(&admit_server)),
                server: Some(server),
                event_loop,
                ..Default::default()
            };
            Client::new(options).unwrap_or_else(|error| arguments::exit(&format!("UCall client: {error}")))
        });
        poll(server, event_loop, client.as_mut(), &upstreams, &batches, poll_slice);
    };
    let until_stopped = || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
        let runtime = runtime.unwrap_or_else(|error| arguments::exit(&format!("signals: {error}")));
        runtime.block_on(arguments::stop_signal());
        server.stop(); // every `take_call` returns, so every pool's threads finish
    };
    serve(&mut pollers, &poll_loop, &until_stopped);
    // Per-loop and per-endpoint counters, so sends, completions and wakeups per request can be checked per row.
    for event_loop in 0..server.event_loops() {
        if let Ok(statistics) = server.event_loop_statistics(event_loop) {
            eprintln!("UCall event loop {event_loop}: {statistics:?}");
        }
    }
    for (index, endpoint) in batches.iter().enumerate() {
        if let Ok(statistics) = endpoint.statistics() {
            eprintln!("UCall batch endpoint {index}: {statistics:?}");
        }
    }
}
