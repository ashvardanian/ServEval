//! tonic's gRPC client: one HTTP/2 channel per connection, opened up front from the paired interface, with every
//! slot multiplexing its calls over its own channels, or opening one per call in the handshake phase.

use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures::StreamExt;
use prost::Message;
use tokio_rustls::TlsConnector;
use tonic::{metadata::MetadataValue, transport::Channel, Code, Request, Status};

use serveval::{
    arguments::{self, BetweenRuns, ClientArgs, Parsing, Phase, Run, Workload},
    payloads::{self, ToolParts},
    proto::{agent_client::AgentClient, Chunk, Empty, ToolCall, ToolReply},
    results::{Pair, Report, Sample},
    security,
    stats::{self, Failure, Slot, Tally, UnitStats, Window},
};

/// tonic driving the tonic server through the tool, context and fan-out workloads.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    client: ClientArgs,
}

type Client = AgentClient<Channel>;

/// Context uploads stream in chunks well under gRPC's default 4MB message limit.
const CHUNK_BYTES: usize = 64 << 10;

struct Shared {
    args: ClientArgs,
    targets: Vec<SocketAddr>,
    tls: Option<TlsConnector>,
    ring: Vec<Call>,
}

/// One ring entry as every gRPC shape sends it, and the replies it must get back.
struct Call {
    message: ToolCall,
    /// The JSON body a `Raw` call sends.
    body: Bytes,
    traceparent: MetadataValue<tonic::metadata::Ascii>,
    reply: ToolReply,
    full_reply: ToolReply,
}

impl Call {
    fn new(parts: &ToolParts) -> Self {
        Self {
            message: parts.message(),
            body: parts.body(),
            traceparent: parts.traceparent().parse().expect("a traceparent is ASCII"),
            reply: payloads::reply_message(&parts.id, parts.check(), ""),
            full_reply: payloads::reply_message(&parts.id, parts.check(), parts.trace_id()),
        }
    }
}

struct Load {
    shared: Arc<Shared>,
    run: Run,
    context: Bytes,
}

fn client(shared: &Shared, target: usize, index: usize) -> Client {
    let interface = shared.args.interface_of(target).clone();
    AgentClient::new(security::tonic_channel(
        interface,
        index,
        shared.targets[target],
        shared.tls.clone(),
    ))
}

/// One call's status, then whether its reply matches, with the payload bytes sent and received.
async fn exchange(load: &Load, client: &mut Client, sequence: usize) -> Result<Option<(u64, u64)>, Status> {
    let length = load.context.len();
    if load.run.workload == Workload::Context {
        let chunks: Vec<Chunk> = (0..length)
            .step_by(CHUNK_BYTES)
            .map(|start| Chunk {
                data: load.context.slice(start..length.min(start + CHUNK_BYTES)),
            })
            .collect();
        let count = client.context(futures::stream::iter(chunks)).await?.into_inner();
        return Ok((count.bytes == length as u64).then_some((length as u64, count.encoded_len() as u64)));
    }
    let call = &load.shared.ring[sequence % payloads::RING];
    let sizes = (payloads::TOOL_BYTES as u64, payloads::REPLY_BYTES as u64);
    let mut request = Request::new(call.message.clone());
    let reply = match (load.run.workload, load.run.parsing) {
        (Workload::Fanout, _) => {
            request.metadata_mut().insert("width", load.shared.args.width.into());
            client.fanout(request).await?.into_inner()
        }
        (_, Parsing::Raw) => {
            let data = call.body.clone();
            let reply = client.raw(Chunk { data: data.clone() }).await?.into_inner();
            let sizes = (data.len() as u64, reply.data.len() as u64);
            return Ok((reply.data == data.slice(..payloads::REPLY_BYTES)).then_some(sizes));
        }
        (_, Parsing::Body) => client.tool(request).await?.into_inner(),
        (_, Parsing::Full) => {
            let metadata = request.metadata_mut();
            metadata.insert(
                "authorization",
                payloads::AUTHORIZATION.parse().expect("the token is ASCII"),
            );
            metadata.insert("traceparent", call.traceparent.clone());
            let reply = client.full(request).await?.into_inner();
            return Ok((reply == call.full_reply).then_some(sizes));
        }
    };
    Ok((reply == call.reply).then_some(sizes))
}

/// Why a call failed: a transport error, a deadline, a status the server sent, or a wrong reply.
fn failure(outcome: Result<Option<(u64, u64)>, Status>) -> Result<(u64, u64), Failure> {
    match outcome {
        Ok(Some(sizes)) => Ok(sizes),
        Ok(None) => Err(Failure::WrongReply),
        Err(status) if std::error::Error::source(&status).is_some() => Err(Failure::of(&status)),
        Err(status) if status.code() == Code::DeadlineExceeded => Err(Failure::Timeout),
        Err(_) => Err(Failure::Status),
    }
}

async fn server_stats(shared: &Shared) -> UnitStats {
    let mut all = Vec::new();
    for target in 0..shared.targets.len() {
        let reply = client(shared, target, 0)
            .stats(Empty {})
            .await
            .map(tonic::Response::into_inner);
        all.push(reply.map_or_else(
            |_| UnitStats::default(),
            |reply| UnitStats {
                cpu_microseconds: reply.cpu_microseconds,
                memory_bytes: reply.memory_bytes,
                cores: reply.cores,
                memory_limit: reply.memory_limit,
                tcp_active_opens: reply.tcp_active_opens,
            },
        ));
    }
    UnitStats::sum(&all)
}

/// Takes every channel from the ones in `kept` and new ones, which `kept` holds from then on, handing slot
/// `target + i * targets` the channels `j` of that target with `j % slots == i`, or channel `i % channels` when slots
/// outnumber channels.
async fn open_all(load: &Load, kept: &mut [Vec<Client>]) -> io::Result<Vec<Vec<Client>>> {
    let (shared, run) = (&load.shared, &load.run);
    let targets = shared.targets.len();
    let mut slots: Vec<Vec<Client>> = (0..run.in_flight).map(|_| Vec::new()).collect();
    for target in 0..targets {
        let slots_here = arguments::share(run.in_flight, target, targets);
        let count = arguments::share(run.connections, target, targets);
        if slots_here == 0 {
            continue;
        }
        if count == 0 {
            return Err(io::Error::other(format!(
                "--connections {} leaves target {target} without a channel; pass at least {targets}",
                run.connections
            )));
        }
        kept[target].truncate(count);
        let opened = futures::stream::iter(kept[target].len()..count).map(|index| async move {
            let mut channel = client(shared, target, index);
            channel.health(Empty {}).await.map(|_| channel)
        });
        for channel in opened.buffered(1024).collect::<Vec<Result<Client, Status>>>().await {
            kept[target].push(channel.map_err(io::Error::other)?);
        }
        let opened = &kept[target];
        for local in 0..slots_here {
            let mine: Vec<Client> = opened.iter().skip(local).step_by(slots_here).cloned().collect();
            slots[target + local * targets] = if mine.is_empty() {
                vec![opened[local % count].clone()]
            } else {
                mine
            };
        }
    }
    Ok(slots)
}

async fn drive(load: Arc<Load>, window: Window, slot: usize, mut clients: Vec<Client>) -> Tally {
    let target = slot % load.shared.targets.len();
    let mut schedule = Slot::new(window, slot);
    let mut turn = 0;
    while let Some((intended, sequence)) = schedule.next().await {
        let outcome = match load.run.phase {
            Phase::Steady => {
                turn += 1;
                let index = turn % clients.len();
                let client = &mut clients[index];
                exchange(&load, client, sequence).await
            }
            Phase::Handshake => exchange(&load, &mut client(&load.shared, target, sequence), sequence).await,
        };
        schedule.record(intended, failure(outcome));
    }
    schedule.tally
}

async fn measure(args: ClientArgs) -> io::Result<()> {
    let ring: Vec<Call> = payloads::tool_ring(args.common.seed.into())
        .iter()
        .map(Call::new)
        .collect();
    assert!(
        ring.iter()
            .all(|call| call.message.encoded_len() == payloads::TOOL_BYTES
                && call.reply.encoded_len() == payloads::REPLY_BYTES
                && call.full_reply.encoded_len() == payloads::REPLY_BYTES),
        "the tool messages lost their padding"
    );
    let shared = Arc::new(Shared {
        targets: args
            .targets
            .iter()
            .map(|&address| SocketAddr::new(address, args.common.port))
            .collect(),
        tls: security::tls_connector(&args.common, &[b"h2"])?,
        ring,
        args,
    });
    let args = &shared.args;
    let health = client(&shared, 0, 0)
        .health(Empty {})
        .await
        .map_err(io::Error::other)?
        .into_inner();
    let pair = Pair {
        framework: health.framework,
        client: health.client,
        batching: health.batching,
    };
    let runs = args.runs(pair.batching, true);
    let mut report = Report::new(args, pair);
    let mut kept: Vec<Vec<Client>> = shared.targets.iter().map(|_| Vec::new()).collect();
    for (number, run) in runs.into_iter().enumerate() {
        let context = match run.workload {
            Workload::Context => payloads::context_body(args.common.seed.into(), run.body_bytes),
            _ => Bytes::new(),
        };
        let load = Arc::new(Load {
            shared: shared.clone(),
            run,
            context,
        });
        if let Err(status) = exchange(&load, &mut client(&shared, 0, 0), 0).await {
            if status.code() == Code::Unimplemented {
                report.unsupported(&run);
                continue;
            }
        }
        let server_before = server_stats(&shared).await;
        let cpu_before = stats::process_cpu();
        let slots = match run.phase {
            Phase::Steady => open_all(&load, &mut kept).await?,
            Phase::Handshake => (0..run.in_flight).map(|_| Vec::new()).collect(),
        };
        let window = Window::new(
            arguments::start_at(number),
            args.warmup,
            args.time_limit,
            args.calls_per_second,
            run.in_flight,
        );
        let opens_before = stats::tcp_active_opens();
        let tasks: Vec<_> = slots
            .into_iter()
            .enumerate()
            .map(|(slot, clients)| tokio::spawn(drive(load.clone(), window, slot, clients)))
            .collect();
        let mut tally = Tally::default();
        for task in tasks {
            tally.merge(&task.await.map_err(io::Error::other)?);
        }
        let opens_after = stats::tcp_active_opens();
        let server_after = server_stats(&shared).await;
        let sample = Sample {
            server_before,
            server_after,
            client_cpu: stats::process_cpu() - cpu_before,
            client_opens: [opens_before, opens_after],
        };
        report.add(args, &run, &window, &tally, &sample)?;
        if args.between_runs == BetweenRuns::Reconnect {
            kept.iter_mut().for_each(Vec::clear);
            let mut earlier = (stats::sockets_in_use(), server_stats(&shared).await.memory_bytes);
            for _ in 0..stats::SETTLE_ROUNDS {
                tokio::time::sleep(stats::SETTLE_INTERVAL).await;
                let later = (stats::sockets_in_use(), server_stats(&shared).await.memory_bytes);
                if stats::settled(earlier, later) {
                    break;
                }
                earlier = later;
            }
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let Cli { client: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "tonic");
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(args.common.threads.0.get());
    runtime.enable_all().build()?.block_on(measure(args))
}
