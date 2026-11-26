//! UCall's Rust client: one client per thread, which lives across configurations so its connections can stay open
//! under `--between-runs keep`, opened up front by concurrent health checks, and driving its share of the slots on the
//! schedule `stats::Slot` keeps, without an async runtime. UCall's client binds neither a device nor an address, so the
//! kernel routes each target by its subnet.
//!
//! UCall pipelines only idempotent calls, per RFC 9112 section 9.3.2, so a run keeping several calls in flight per
//! connection sends its tool calls as PUT, which the UCall servers answer as they answer POST; context uploads already
//! are. A fan-out's POST is not idempotent, so its deeper runs, and handshake runs, whose calls close their connection,
//! are `unsupported`.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::BinaryHeap,
    net::SocketAddr,
    path::Path,
    sync::{mpsc, Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use ucall::{
    BodyDelivery, Client, ClientCall, ClientCallOptions, ClientOptions, Completed, HTTPStatus, NoiseKey,
    PeerAuthorizer, Response, Status, Verb,
};

use serveval::{
    arguments::{self, BetweenRuns, ClientArgs, Phase, Run, Security, Workload},
    payloads::{self, ToolParts},
    results::{Pair, Report, Sample},
    stats::{self, Failure, ServerStats, Tally, UnitStats, Window},
};

const JSON: &[u8] = b"content-type: application/json";
const IDENTITY: &[u8] = b"accept-encoding: identity";
const CLOSE: &[u8] = b"connection: close";
/// Why a fan-out run keeping several calls in flight per connection cannot run.
const FANOUT_NOT_IDEMPOTENT: &str = "fan-out calls are not idempotent";
/// Why a handshake run keeping several calls in flight per connection cannot run: UCall pipelines no call behind one
/// that closes its connection.
const HANDSHAKE_CLOSES: &str = "handshake calls close their connection";

/// The requests a UCall server lets one connection pipeline, from the same `/health` reply as `Pair`; servers that do
/// not pipeline leave it out.
#[derive(serde::Deserialize)]
struct Pipelining {
    #[serde(default)]
    max_pipelined_requests_per_connection: Option<u32>,
}

/// UCall driving a UCall server through the tool, context, download and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    client: ClientArgs,
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

/// Bytes kept for the whole process, as every call UCall admits borrows its URL, headers and body until it completes.
fn keep<T: ?Sized>(owned: Box<T>) -> &'static T {
    Box::leak(owned)
}

/// Everything every thread shares.
struct Shared {
    args: ClientArgs,
    /// `http://ADDRESS:PORT` or `http+noise://…` per target.
    origins: Vec<String>,
    security: ucall::Security,
    /// This client's private key and the server's public one under Noise.
    keys: Option<(NoiseKey, NoiseKey)>,
    ring: Vec<ToolParts>,
    /// A context body per `--body-bytes` size, which an upload sends and a download must get back.
    contexts: Vec<&'static [u8]>,
    /// The largest reply a call buffers when downloads are swept: the largest download and its reply's head, set
    /// once from the servers' own reply; unset, UCall's default.
    max_response_bytes: std::sync::OnceLock<usize>,
}

impl Shared {
    fn options(&self) -> ClientCallOptions {
        ClientCallOptions {
            security: self.security,
            noise_peer_key: self.keys.map(|(_, server)| server),
            ..Default::default()
        }
    }

    /// A client of `connections` connections, each pipelining up to `depth` of the `calls` it accepts at once,
    /// admitting the servers' key under Noise XX.
    fn client<'handlers, F: Fn(&NoiseKey) -> bool + Sync>(
        &self,
        (connections, depth, calls): (usize, usize, usize),
        admit: &'handlers F,
    ) -> Client<'handlers> {
        let options = ClientOptions {
            connections,
            pipelined_calls: depth,
            calls,
            noise_private_key: self.keys.map(|(client, _)| client),
            peer_authorizer: self.keys.map(|_| PeerAuthorizer::new(admit)),
            max_response_bytes: self.max_response_bytes.get().copied().unwrap_or_default(),
            ..Default::default()
        };
        Client::new(options).unwrap_or_else(|error| arguments::exit(&format!("UCall client: {error}")))
    }

    fn get(&self, target: usize, path: &str) -> ClientCall {
        let mut call = ClientCall::get(format!("{}{path}", self.origins[target]));
        call.headers = vec![Cow::Borrowed(IDENTITY), Cow::Borrowed(CLOSE)];
        call.options = self.options();
        call
    }
}

/// One call as every thread sends it to one target, and the reply it must get back byte for byte.
struct Exchange {
    call: ClientCall,
    reply: &'static [u8],
}

/// One configuration's calls: per target, one exchange per ring entry, or the one context upload or download.
struct Load {
    run: Run,
    exchanges: Vec<Vec<Exchange>>,
}

impl Load {
    fn new(shared: &Shared, run: Run) -> Self {
        let args = &shared.args;
        let mut headers = vec![Cow::Borrowed(JSON), Cow::Borrowed(IDENTITY)];
        if run.phase == Phase::Handshake {
            headers.push(Cow::Borrowed(CLOSE));
        }
        let exchange = |target: usize, verb: Verb, path: &str, extra: Vec<Cow<'static, [u8]>>, body, reply| Exchange {
            call: ClientCall {
                verb,
                url: Cow::Borrowed(keep(format!("{}{path}", shared.origins[target]).into_boxed_str())),
                headers: [headers.clone(), extra].concat(),
                body: Cow::Borrowed(body),
                options: shared.options(),
            },
            reply,
        };
        // Only idempotent calls pipeline, so deeper runs send tool calls as PUT.
        let verb = match run.calls_in_flight_per_connection {
            1 => Verb::Post,
            _ => Verb::Put,
        };
        let position = args.body_bytes().iter().position(|&bytes| bytes == run.body_bytes);
        let context = || shared.contexts[position.expect("every size has a body")];
        let exchanges = (0..shared.origins.len()).map(|target| match run.workload {
            Workload::Context => {
                let reply = keep(payloads::count_reply(run.body_bytes).into_bytes().into_boxed_slice());
                vec![exchange(target, Verb::Put, "/context", Vec::new(), context(), reply)]
            }
            Workload::Download => {
                let path = payloads::download_path(run.body_bytes);
                vec![exchange(target, Verb::Get, &path, Vec::new(), &[], context())]
            }
            _ => payloads::exchanges(&shared.ring, &run, args.width)
                .into_iter()
                .map(|parts| {
                    let extra = match &parts.traceparent {
                        Some(traceparent) => vec![
                            Cow::Owned(format!("authorization: {}", payloads::AUTHORIZATION).into_bytes()),
                            Cow::Owned(format!("traceparent: {traceparent}").into_bytes()),
                        ],
                        None => Vec::new(),
                    };
                    let body = keep(parts.body.to_vec().into_boxed_slice());
                    let reply = keep(parts.reply.to_vec().into_boxed_slice());
                    exchange(target, verb, &parts.path, extra, body, reply)
                })
                .collect(),
        });
        Self {
            run,
            exchanges: exchanges.collect(),
        }
    }

    fn exchange(&self, target: usize, sequence: usize) -> &Exchange {
        let exchanges = &self.exchanges[target];
        &exchanges[sequence % exchanges.len()]
    }
}

/// Why a call UCall finished without a matching reply failed.
fn failure_of(status: Status) -> Failure {
    match status {
        Status::Timeout => Failure::Timeout,
        _ => Failure::Connect,
    }
}

/// RFC 9112's status line of a 200 reply, with RFC 9110's reason phrase, as UCall's servers write it.
const STATUS_LINE_OK: &str = "HTTP/1.1 200 OK\r\n";

/// The bytes of a 200 reply's head as RFC 9112 frames it: the status line, every `name: value` field line, and the
/// empty line ending the head.
fn head_bytes(response: &Response) -> usize {
    let fields = response
        .fields()
        .map(|(name, value)| name.len() + ": ".len() + value.len() + "\r\n".len());
    STATUS_LINE_OK.len() + fields.sum::<usize>() + "\r\n".len()
}

/// The largest download and the head of the servers' reply to it, from one streamed download of that size whose body
/// is pulled and dropped, so every buffered download fits; `None` when the server does not answer it with 200. It
/// sends a steady call's headers, whose reply's `connection: keep-alive` is the longer of the two forms.
fn largest_reply_bytes(shared: &Shared, control: &mut Client, largest: u64) -> Option<usize> {
    let mut call = shared.get(0, &payloads::download_path(largest));
    call.headers = vec![Cow::Borrowed(JSON), Cow::Borrowed(IDENTITY)];
    call.options.body_delivery = BodyDelivery::Streamed;
    let mut response = control.call(call).ok()?;
    let head = head_bytes(&response);
    let answered = response.status_code() == HTTPStatus::OK;
    while response.body_chunk().is_ok_and(|chunk| !chunk.is_empty()) {}
    answered.then_some(largest as usize + head)
}

/// The body bytes sent and received by a finished call, or why it failed.
fn verify(completed: Completed, exchange: &Exchange) -> Result<(u64, u64), Failure> {
    let response = completed.response.map_err(|error| failure_of(error.status))?;
    let body = response.body().map_err(|error| failure_of(error.status))?;
    match (response.status_code() == HTTPStatus::OK, body == exchange.reply) {
        (false, _) => Err(Failure::Status),
        (true, false) => Err(Failure::WrongReply),
        (true, true) => Ok((exchange.call.body.len() as u64, body.len() as u64)),
    }
}

/// A call of one slot that UCall holds.
#[derive(Clone, Copy)]
struct Flight {
    intended: Instant,
    sequence: usize,
}

/// One thread's slots `thread + i * threads` of the window, slot `i` on target `i % targets`, each with one call
/// outstanding: `stats::Slot` schedules them, here without an async runtime, so UCall's client waits on the network
/// exactly until the next call is due.
struct Slots<'load> {
    load: &'load Load,
    window: Window,
    threads: usize,
    slots: Vec<usize>,
    arrivals: Vec<usize>,
    flights: Vec<Flight>,
    due: BinaryHeap<Reverse<(Instant, usize, usize)>>,
    tally: Tally,
}

impl<'load> Slots<'load> {
    fn new(load: &'load Load, window: Window, thread: usize, threads: usize) -> Self {
        let slots: Vec<usize> = (thread..window.slots).step_by(threads).collect();
        let idle = Flight {
            intended: window.begin,
            sequence: 0,
        };
        Self {
            load,
            window,
            threads,
            arrivals: vec![0; slots.len()],
            flights: vec![idle; slots.len()],
            due: BinaryHeap::new(),
            tally: Tally::default(),
            slots,
        }
    }

    fn scheduled(&self, sequence: usize) -> Option<Instant> {
        let rate = self.window.calls_per_second?;
        Some(self.window.begin + Duration::from_secs_f64(sequence as f64 / rate as f64))
    }

    /// Queues the next call of slot `position`, or counts the measured calls it owed once the window closed.
    fn queue(&mut self, position: usize) {
        let sequence = self.slots[position] + self.arrivals[position] * self.window.slots;
        self.arrivals[position] += 1;
        let intended = self.scheduled(sequence).unwrap_or_else(Instant::now);
        if intended < self.window.until && Instant::now() < self.window.until {
            self.due.push(Reverse((intended, position, sequence)));
            return;
        }
        let owed = (sequence..)
            .step_by(self.window.slots)
            .map_while(|late| self.scheduled(late));
        let owed = owed.take_while(|&late| late < self.window.until);
        self.tally.unsent += owed.filter(|&late| late >= self.window.measured_from).count() as u64;
    }

    fn record(&mut self, flight: Flight, outcome: Result<(u64, u64), Failure>) {
        let tally = &mut self.tally;
        tally.all_calls += 1;
        if flight.intended < self.window.measured_from {
            return;
        }
        match outcome {
            Ok((sent, received)) => {
                tally.calls += 1;
                tally.request_body_bytes += sent;
                tally.response_body_bytes += received;
                tally.latencies.record(flight.intended.elapsed());
            }
            Err(failure) => tally.failures.count(failure),
        }
    }

    fn target(&self, position: usize) -> usize {
        self.slots[position] / self.threads % self.load.exchanges.len()
    }

    fn run(mut self, client: &mut Client) -> Tally {
        thread::sleep(self.window.begin.saturating_duration_since(Instant::now()));
        (0..self.slots.len()).for_each(|position| self.queue(position));
        let mut completed: Vec<Completed> = (0..self.slots.len()).map(|_| Completed::default()).collect();
        let mut outstanding = 0;
        while outstanding != 0 || !self.due.is_empty() {
            let now = Instant::now();
            while let Some(&Reverse((intended, position, sequence))) = self.due.peek() {
                if intended > now {
                    break;
                }
                self.due.pop();
                if intended >= self.window.measured_from {
                    self.tally.lateness.record(now - intended);
                }
                let flight = Flight { intended, sequence };
                let call = self.load.exchange(self.target(position), sequence).call.clone();
                match client.submit(call, position as u64) {
                    Ok(()) => {
                        self.flights[position] = flight;
                        outstanding += 1;
                    }
                    Err(error) => {
                        self.record(flight, Err(failure_of(error.status)));
                        self.queue(position);
                    }
                }
            }
            let wait = self
                .due
                .peek()
                .map(|Reverse((intended, ..))| intended.saturating_duration_since(now));
            if outstanding == 0 {
                thread::sleep(wait.unwrap_or_default());
                continue;
            }
            let Ok(finished) = client.take_completed(&mut completed, wait.unwrap_or(Duration::MAX)) else {
                continue;
            };
            for done in finished.iter_mut() {
                let position = done.context as usize;
                let flight = self.flights[position];
                let exchange = self.load.exchange(self.target(position), flight.sequence);
                let outcome = verify(std::mem::take(done), exchange);
                outstanding -= 1;
                self.record(flight, outcome);
                self.queue(position);
            }
        }
        self.tally
    }
}

/// What a thread receives for one configuration and reports back.
struct Steps {
    loads: mpsc::Receiver<Arc<Load>>,
    windows: mpsc::Receiver<Window>,
    ready: mpsc::Sender<()>,
    finished: mpsc::Sender<Tally>,
    sampled: Arc<Barrier>,
}

/// One thread's client across configurations: it opens its share of the connections, reports `ready`, runs the
/// window it then receives, sends its tally, and keeps its connections open until the caller has sampled the servers;
/// it reuses the client for the next load when the policy and its connection share allow.
fn drive_thread(shared: Arc<Shared>, thread: usize, threads: usize, steps: Steps) {
    let server_key = shared.keys.map(|(_, server)| server);
    let admit = move |key: &NoiseKey| Some(*key) == server_key;
    let mut kept: Option<(Client, (usize, usize, usize))> = None;
    while let Ok(load) = steps.loads.recv() {
        let run = &load.run;
        let connections = arguments::share(run.connections, thread, threads).max(1);
        let depth = run.calls_in_flight_per_connection;
        // The slots this thread drives, which may exceed its connections' depth by the remainders of the two splits.
        let calls = arguments::share(run.in_flight, thread, threads).max(connections * depth);
        let budget = (connections, depth, calls);
        if kept.as_ref().is_none_or(|(_, kept)| *kept != budget) {
            drop(kept.take());
            kept = Some((shared.client(budget, &admit), budget));
        }
        let (client, _) = kept.as_mut().expect("made above");
        if run.phase == Phase::Steady {
            let targets = shared.origins.len();
            for target in 0..targets {
                for _ in 0..arguments::share(connections, target, targets) {
                    let _ = client.submit(shared.get(target, "/health"), 0);
                }
            }
            let mut completed: Vec<Completed> = (0..connections).map(|_| Completed::default()).collect();
            let mut answered = 0;
            while answered < connections {
                match client.take_completed(&mut completed, Duration::MAX) {
                    Ok(finished) => answered += finished.len(),
                    Err(_) => break,
                }
            }
        }
        let _ = steps.ready.send(());
        let Ok(window) = steps.windows.recv() else {
            return;
        };
        let _ = steps
            .finished
            .send(Slots::new(&load, window, thread, threads).run(client));
        steps.sampled.wait();
        if shared.args.between_runs == BetweenRuns::Reconnect {
            kept = None;
        }
    }
}

/// The servers' cgroup counters, summed over the targets.
fn server_stats(shared: &Shared, client: &mut Client) -> ServerStats {
    let all: Vec<UnitStats> = (0..shared.origins.len())
        .map(|target| {
            let response = client.call(shared.get(target, "/stats")).ok();
            let body = response.as_ref().and_then(|response| response.body().ok());
            body.and_then(|body| serde_json::from_slice(body).ok())
                .unwrap_or_default()
        })
        .collect();
    UnitStats::sum(&all)
}

/// Exits on a report that could not be written.
fn unwritable(args: &ClientArgs, error: &std::io::Error) -> ! {
    arguments::exit(&format!(
        "--output=\"{}\" is not writable: {error}",
        args.output.display()
    ))
}

fn main() {
    let Cli { client: args } = arguments::parse_cli();
    let security = noise_of(args.common.security);
    let keys = (security != ucall::Security::None).then(|| {
        let data_dir = &args.common.data_dir;
        (read_key(data_dir, "client.key"), read_key(data_dir, "server.pub"))
    });
    let scheme = match security {
        ucall::Security::None => "http",
        _ => "http+noise",
    };
    let bodies = [Workload::Context, Workload::Download];
    let contexts = match args.workloads.iter().any(|workload| bodies.contains(workload)) {
        true => (args.body_bytes().iter())
            .map(|&bytes| {
                keep(
                    payloads::context_body(args.common.seed.into(), bytes)
                        .to_vec()
                        .into_boxed_slice(),
                )
            })
            .collect(),
        false => Vec::new(),
    };
    let shared = Arc::new(Shared {
        origins: (args.targets.iter())
            .map(|&address| format!("{scheme}://{}", SocketAddr::new(address, args.common.port)))
            .collect(),
        security,
        keys,
        ring: payloads::tool_ring(args.common.seed.into()),
        contexts,
        max_response_bytes: std::sync::OnceLock::new(),
        args,
    });
    let args = &shared.args;
    let server_key = shared.keys.map(|(_, server)| server);
    let admit = move |key: &NoiseKey| Some(*key) == server_key;
    let targets = shared.origins.len();
    let mut control = shared.client((targets, 1, targets), &admit);
    let health = control.call(shared.get(0, "/health"));
    let health = health.as_ref().ok().and_then(|response| response.body().ok());
    let (served, pipelining): (Pair, Pipelining) = health
        .and_then(|health| {
            Some((
                serde_json::from_slice(health).ok()?,
                serde_json::from_slice(health).ok()?,
            ))
        })
        .unwrap_or_else(|| arguments::exit("no /health reply"));
    // The row names this client, not the one `/health` names for the server's own pair.
    let pair = Pair {
        client: "ucall".into(),
        ..served
    };
    let largest = match args.workloads.contains(&Workload::Download) {
        true => args.body_bytes().iter().copied().max(),
        false => None,
    };
    if let Some(bytes) = largest.and_then(|largest| largest_reply_bytes(&shared, &mut control, largest)) {
        let _ = shared.max_response_bytes.set(bytes);
        control = shared.client((targets, 1, targets), &admit);
    }
    // `server-maximum` is the server's pipelining depth, or, from a server that reports none, its stream limit.
    let advertised = (pipelining.max_pipelined_requests_per_connection).or(pair.max_concurrent_streams_per_connection);
    let mut runs = args.runs(pair.batching, true, advertised);
    for run in runs.iter_mut().filter(|run| run.calls_in_flight_per_connection > 1) {
        run.unsupported = match (run.workload, run.phase) {
            (Workload::Fanout, _) => Some(FANOUT_NOT_IDEMPOTENT),
            (_, Phase::Handshake) => Some(HANDSHAKE_CLOSES),
            _ => None,
        };
    }
    let mut report = Report::new(args, pair);
    let threads = args.common.threads.0.get();
    let (ready, readied) = mpsc::channel();
    let (finished, tallies) = mpsc::channel();
    let sampled = Arc::new(Barrier::new(threads + 1));
    let (senders, workers): (Vec<_>, Vec<_>) = (0..threads)
        .map(|thread| {
            let (load, loads) = mpsc::channel();
            let (window, windows) = mpsc::channel();
            let steps = Steps {
                loads,
                windows,
                ready: ready.clone(),
                finished: finished.clone(),
                sampled: sampled.clone(),
            };
            let shared = shared.clone();
            (
                (load, window),
                thread::spawn(move || drive_thread(shared, thread, threads, steps)),
            )
        })
        .unzip();
    drop((ready, finished));
    for (number, run) in runs.into_iter().enumerate() {
        if let Some(reason) = run.unsupported {
            if let Err(error) = report.unsupported(args, &run, reason) {
                unwritable(args, &error);
            }
            continue;
        }
        let load = Arc::new(Load::new(&shared, run));
        let probe = control
            .call(load.exchange(0, 0).call.clone())
            .map(|response| response.status_code());
        if let Ok(HTTPStatus::NOT_FOUND | HTTPStatus::METHOD_NOT_ALLOWED | HTTPStatus::NOT_IMPLEMENTED) = probe {
            if let Err(error) = report.unsupported(args, &run, arguments::NOT_SERVED) {
                unwritable(args, &error);
            }
            continue;
        }
        let server_before = server_stats(&shared, &mut control);
        let cpu_before = stats::process_cpu();
        senders
            .iter()
            .for_each(|(sender, _)| sender.send(load.clone()).expect("a worker waiting for its load"));
        readied.iter().take(threads).for_each(drop);
        let window = Window::new(
            arguments::start_at(number),
            args.warmup,
            args.time_limit,
            args.calls_per_second,
            run.in_flight,
        );
        let opens_before = stats::tcp_active_opens();
        senders
            .iter()
            .for_each(|(_, sender)| sender.send(window).expect("a worker waiting for its window"));
        let mut tally = Tally::default();
        tallies.iter().take(threads).for_each(|other| tally.merge(&other));
        let opens_after = stats::tcp_active_opens();
        let server_after = server_stats(&shared, &mut control);
        sampled.wait();
        let sample = Sample {
            server_before,
            server_after,
            client_cpu: stats::process_cpu() - cpu_before,
            client_opens: [opens_before, opens_after],
        };
        if let Err(error) = report.add(args, &run, &window, &tally, &sample) {
            unwritable(args, &error);
        }
        if args.between_runs == BetweenRuns::Reconnect {
            let mut earlier = (
                stats::sockets_in_use(),
                server_stats(&shared, &mut control).memory_current_bytes,
            );
            for _ in 0..stats::SETTLE_ROUNDS {
                thread::sleep(stats::SETTLE_INTERVAL);
                let later = (
                    stats::sockets_in_use(),
                    server_stats(&shared, &mut control).memory_current_bytes,
                );
                if stats::settled(earlier, later) {
                    break;
                }
                earlier = later;
            }
        }
    }
    drop(senders);
    for worker in workers {
        if worker.join().is_err() {
            arguments::exit("a client thread panicked");
        }
    }
}
