//! tonic on a multi-threaded Tokio runtime, terminating TLS through a rustls acceptor, and stopping on SIGINT or
//! SIGTERM.

use std::{io, pin::Pin};

use futures::{future::try_join_all, Stream};
use tonic::{Request, Response, Status, Streaming};

use serveval::{
    arguments::{self, DownloadBody, ServerArgs},
    payloads,
    proto::{
        agent_client::AgentClient,
        agent_server::{Agent, AgentServer},
        Chunk, Count, Empty, HealthReply, StatsReply, ToolCall, ToolReply,
    },
    security, stats,
};

/// tonic serving the tool, context, download and fan-out workloads over gRPC.
#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    #[command(flatten)]
    server: ServerArgs,
}

/// The `SETTINGS_MAX_CONCURRENT_STREAMS` the server builder is configured with. tonic 0.14's `Server::builder`
/// defaults its `max_concurrent_streams` to `None` and always hands that to hyper's HTTP/2 builder, replacing hyper's
/// own default of 200; hyper then leaves h2's setting unset, so the server sends no limit in its SETTINGS frame and
/// `/health` advertises none.
const MAX_CONCURRENT_STREAMS: Option<u32> = None;

struct Service {
    upstreams: Vec<AgentClient<tonic::transport::Channel>>,
    /// The bytes every download is a prefix of.
    prepared: std::sync::Arc<DownloadBody>,
}

#[tonic::async_trait]
impl Agent for Service {
    async fn tool(&self, request: Request<ToolCall>) -> Result<Response<ToolReply>, Status> {
        let call = request.into_inner();
        Ok(Response::new(payloads::reply_message(&call.id, call.check(), "")))
    }

    async fn raw(&self, request: Request<Chunk>) -> Result<Response<Chunk>, Status> {
        let data = request.into_inner().data;
        Ok(Response::new(Chunk {
            data: data.slice(..payloads::REPLY_BYTES.min(data.len())),
        }))
    }

    async fn full(&self, request: Request<ToolCall>) -> Result<Response<ToolReply>, Status> {
        let metadata = request.metadata();
        if metadata.get("authorization").map(|value| value.as_bytes()) != Some(payloads::AUTHORIZATION.as_bytes()) {
            return Err(Status::unauthenticated("the bearer token does not match"));
        }
        let traceparent = metadata.get("traceparent").map_or(&b""[..], |value| value.as_bytes());
        let trace = payloads::trace_id(traceparent)
            .ok_or_else(|| Status::invalid_argument("the traceparent metadata entry does not parse"))?
            .to_owned();
        let call = request.into_inner();
        Ok(Response::new(payloads::reply_message(&call.id, call.check(), &trace)))
    }

    async fn context(&self, request: Request<Streaming<Chunk>>) -> Result<Response<Count>, Status> {
        let mut chunks = request.into_inner();
        let mut bytes = 0u64;
        while let Some(chunk) = chunks.message().await? {
            bytes += chunk.data.len() as u64;
        }
        Ok(Response::new(Count { bytes }))
    }

    type DownloadStream = Pin<Box<dyn Stream<Item = Result<Chunk, Status>> + Send>>;

    async fn download(&self, request: Request<Count>) -> Result<Response<Self::DownloadStream>, Status> {
        let bytes = request.into_inner().bytes;
        let prepared = (self.prepared.get()).ok_or_else(|| Status::internal("the server prepared no download body"))?;
        let body = payloads::download(prepared, bytes)
            .ok_or_else(|| Status::out_of_range("the server prepared fewer download bytes"))?;
        let length = body.len();
        let chunks = (0..length).step_by(payloads::CHUNK_BYTES).map(move |start| {
            Ok(Chunk {
                data: body.slice(start..length.min(start + payloads::CHUNK_BYTES)),
            })
        });
        Ok(Response::new(Box::pin(futures::stream::iter(chunks))))
    }

    async fn fanout(&self, request: Request<ToolCall>) -> Result<Response<ToolReply>, Status> {
        let width = request
            .metadata()
            .get("width")
            .and_then(|width| width.to_str().ok()?.parse::<usize>().ok());
        let width = width.ok_or_else(|| Status::invalid_argument("the width metadata entry is missing"))?;
        if self.upstreams.is_empty() {
            return Err(Status::unavailable("started without --upstreams"));
        }
        let call = request.into_inner();
        let expected = payloads::reply_message(&call.id, call.check(), "");
        let gathered = self.upstreams.iter().cycle().take(width).map(|upstream| {
            let (mut upstream, forwarded, expected) = (upstream.clone(), call.clone(), &expected);
            async move {
                let reply = upstream.tool(forwarded).await?.into_inner();
                (&reply == expected)
                    .then_some(())
                    .ok_or_else(|| Status::data_loss("an upstream reply does not match the call"))
            }
        });
        try_join_all(gathered).await?;
        Ok(Response::new(expected))
    }

    async fn health(&self, _: Request<Empty>) -> Result<Response<HealthReply>, Status> {
        let mut reply = Response::new(HealthReply {
            framework: "tonic".into(),
            client: "tonic".into(),
            batching: false,
        });
        // `HealthReply` has no field for the stream limit, so it travels as metadata, left out for no limit.
        if let Some(streams) = MAX_CONCURRENT_STREAMS {
            reply
                .metadata_mut()
                .insert(payloads::MAX_CONCURRENT_STREAMS_METADATA, streams.into());
        }
        Ok(reply)
    }

    async fn stats(&self, _: Request<Empty>) -> Result<Response<StatsReply>, Status> {
        Ok(stats::unit_stats().into_response())
    }
}

fn main() -> io::Result<()> {
    let Cli { server: args } = arguments::parse_cli();
    security::require_tls_or_none(&args.common, "tonic");
    let interface = args.single_interface("tonic").clone();
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(args.common.threads.0.get());
    runtime.enable_all().build()?.block_on(async {
        let listener = arguments::listen(&interface, args.common.port, args.admission())?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let tls = security::tls_connector(&args.common, &[b"h2"])?;
        let upstreams = args.upstreams.iter().enumerate().map(|(index, &address)| {
            AgentClient::new(security::tonic_channel(interface.clone(), index, address, tls.clone()))
        });
        let service = AgentServer::new(Service {
            upstreams: upstreams.collect(),
            prepared: args.download_body(),
        });
        let router = tonic::transport::Server::builder()
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .add_service(service);
        let workers = tokio::runtime::Handle::current().metrics().num_workers();
        println!(
            "Listening on {interface} port {} with {workers} Tokio workers",
            args.common.port
        );
        let served = match security::tls_server_config(&args.common, &[b"h2"])? {
            None => {
                let accepted = futures::stream::unfold(listener, |listener| async move {
                    Some((listener.accept().await.map(|(stream, _)| stream), listener))
                });
                router
                    .serve_with_incoming_shutdown(accepted, arguments::stop_signal())
                    .await
            }
            Some(config) => {
                let accepted = security::TlsListener::new(listener, config)?.into_stream();
                router
                    .serve_with_incoming_shutdown(accepted, arguments::stop_signal())
                    .await
            }
        };
        served.map_err(io::Error::other)
    })
}
