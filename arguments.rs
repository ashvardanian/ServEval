//! Flags shared by every Rust server and client, each defaulting to its `SERVEVAL_<FLAG>` variable.

use std::{
    fmt,
    hash::{BuildHasher, Hasher},
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::ValueEnum;
use socket2::{Domain, Protocol, Socket, Type};

/// Flags every server and client reads.
#[derive(clap::Args)]
pub struct CommonArgs {
    /// Workers, 0 for every core in the affinity mask.
    #[arg(long, env = "SERVEVAL_THREADS", default_value = "0", value_parser = parse_threads_flag)]
    pub threads: Threads,
    /// Local interfaces as DEVICE=ADDRESS or ADDRESS, extra addresses joined by +, like ens5=10.0.1.5,ens6=10.0.2.5.
    #[arg(long, env = "SERVEVAL_INTERFACES", value_delimiter = ',', default_value = "127.0.0.1", value_parser = parse_interface_flag)]
    pub interfaces: Vec<Interface>,
    /// Port every server listens on.
    #[arg(long, env = "SERVEVAL_PORT", default_value = "8545", value_parser = parse_port_flag)]
    pub port: u16,
    #[arg(long, env = "SERVEVAL_SECURITY", value_enum, default_value = "none")]
    pub security: Security,
    /// Directory holding the `tls` and `noise` keys that keys.sh writes.
    #[arg(long, env = "SERVEVAL_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    /// Seed of every payload, or `random` to draw one.
    #[arg(long, env = "SERVEVAL_SEED", default_value = "42", value_parser = parse_seed_flag)]
    pub seed: Seed,
}

/// A server's flags.
#[derive(clap::Args)]
pub struct ServerArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Connection counts, like 64,10K,1M; the largest is the admission budget and listen backlog.
    #[arg(long, env = "SERVEVAL_CONNECTIONS", value_delimiter = ',', default_value = "10K", value_parser = parse_count_flag)]
    pub connections: Vec<usize>,
    /// Fan-out targets as ADDRESS:PORT.
    #[arg(long, env = "SERVEVAL_UPSTREAMS", value_delimiter = ',')]
    pub upstreams: Vec<SocketAddr>,
    /// Most calls one batch gathers, for frameworks that ship batching.
    #[arg(long, env = "SERVEVAL_BATCH_SIZE", default_value = "32", value_parser = parse_count_flag)]
    pub batch_size: usize,
    /// Longest a partial batch's oldest call waits before it fires. The UCall servers fire it on the next poll after
    /// that, at most one poll slice later: the smaller of this wait and `POLL_SLICE`, 100 ms.
    #[arg(long, env = "SERVEVAL_BATCH_WAIT", default_value = "10ms", value_parser = parse_duration_flag)]
    pub batch_wait: Duration,
    #[command(flatten)]
    pub body_bytes: BodyBytes,
}

impl ServerArgs {
    pub fn admission(&self) -> usize {
        self.connections.iter().copied().max().unwrap_or(1)
    }

    /// The bytes every download is a prefix of, for the largest `--body-bytes` of this server's interfaces,
    /// prepared on the first download, so a server that never serves one never derives the sweep.
    pub fn download_body(&self) -> std::sync::Arc<DownloadBody> {
        std::sync::Arc::new(DownloadBody {
            seed: self.common.seed.into(),
            given: self.body_bytes.given.clone(),
            interfaces: self.common.interfaces.clone(),
            prepared: std::sync::OnceLock::new(),
        })
    }

    /// The one interface a competitor process binds; launch.py starts one process per memory domain.
    pub fn single_interface(&self, framework: &str) -> &Interface {
        match self.common.interfaces.as_slice() {
            [interface] => interface,
            _ => exit(&format!(
                "{framework} binds one interface per process; launch.py starts one per memory domain"
            )),
        }
    }
}

/// `--body-bytes`, the context upload and download sizes, shared by clients and servers: a server prepares the largest
/// once and serves every download from it.
#[derive(clap::Args)]
pub struct BodyBytes {
    /// Context upload and download sizes to sweep, like 256KB,4MB,64MB; unset, powers of 4 from one TCP segment's
    /// payload at the interfaces' MTU up to `net.ipv4.tcp_rmem`'s maximum.
    #[arg(long = "body-bytes", env = "SERVEVAL_BODY_BYTES", value_delimiter = ',', value_parser = parse_size_flag)]
    given: Vec<u64>,
    #[arg(skip)]
    resolved: std::sync::OnceLock<Vec<u64>>,
}

impl BodyBytes {
    /// The sizes given, or `default_body_bytes` of `interfaces`, derived on first use; exits when they cannot be.
    pub fn sizes(&self, interfaces: &[Interface]) -> &[u64] {
        self.resolved.get_or_init(|| match self.given.is_empty() {
            false => self.given.clone(),
            true => default_body_bytes(interfaces).unwrap_or_else(|message| exit(&message)),
        })
    }
}

/// The bytes every download of a server is a prefix of, prepared once, on the first download.
pub struct DownloadBody {
    seed: u64,
    given: Vec<u64>,
    interfaces: Vec<Interface>,
    prepared: std::sync::OnceLock<Option<bytes::Bytes>>,
}

impl DownloadBody {
    /// The prepared bytes, `None`, said once on stderr, when the sweep is neither given nor derivable.
    pub fn get(&self) -> Option<&bytes::Bytes> {
        let prepare = || {
            let sizes = match self.given.is_empty() {
                false => Ok(self.given.clone()),
                true => default_body_bytes(&self.interfaces),
            };
            let sizes = sizes.inspect_err(|message| eprintln!("downloads: {message}")).ok()?;
            let largest = sizes.iter().copied().max()?;
            Some(crate::payloads::context_body(self.seed, largest))
        };
        self.prepared.get_or_init(prepare).as_ref()
    }
}

/// RFC 791's IPv4 header without options, RFC 8200's fixed IPv6 header, and RFC 9293's TCP header without options.
const IPV4_HEADER_BYTES: u64 = 20;
const IPV6_HEADER_BYTES: u64 = 40;
const TCP_HEADER_BYTES: u64 = 20;
/// The one upload size of the runs before the sweep, kept in the default sweep so new rows compare with theirs.
pub const EARLIER_UPLOAD_BYTES: u64 = 256 << 10;
/// RFC 7323's timestamps option, 10 bytes padded to 12, which Linux puts on every segment while
/// `net.ipv4.tcp_timestamps` is on.
const TCP_TIMESTAMPS_OPTION_BYTES: u64 = 12;

/// Powers of 4 times the payload one TCP segment carries, from that payload up to `net.ipv4.tcp_rmem`'s maximum, the
/// most one socket's receive buffer grows to, with `EARLIER_UPLOAD_BYTES` among them. The payload is the smallest MTU
/// of the interfaces' devices, from `/sys/class/net/<device>/mtu`, less the IP and TCP headers and the timestamps
/// option when it is on. An interface given as an address alone names the device holding that address, and a
/// wildcard address every device.
pub fn default_body_bytes(interfaces: &[Interface]) -> Result<Vec<u64>, String> {
    let payloads = interfaces
        .iter()
        .map(segment_payload)
        .collect::<Result<Vec<u64>, String>>()?;
    let mut payload = payloads
        .into_iter()
        .min()
        .ok_or("no interface is given; pass --body-bytes")?;
    if read_setting("/proc/sys/net/ipv4/tcp_timestamps")?.trim() != "0" {
        payload = payload.saturating_sub(TCP_TIMESTAMPS_OPTION_BYTES);
    }
    let receive_buffer = read_setting("/proc/sys/net/ipv4/tcp_rmem")?;
    let largest: Option<u64> = receive_buffer
        .split_whitespace()
        .nth(2)
        .and_then(|text| text.parse().ok());
    let largest = largest.ok_or("/proc/sys/net/ipv4/tcp_rmem holds no maximum")?;
    if payload == 0 {
        return Err("the interfaces' MTU leaves no TCP payload; pass --body-bytes".into());
    }
    let powers = std::iter::successors(Some(payload), |&size| size.checked_mul(4));
    let mut sizes: Vec<u64> = powers.take_while(|&size| size <= largest).collect();
    if sizes.is_empty() {
        sizes.push(payload);
    }
    if !sizes.contains(&EARLIER_UPLOAD_BYTES) {
        sizes.push(EARLIER_UPLOAD_BYTES);
        sizes.sort_unstable();
    }
    Ok(sizes)
}

fn read_setting(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("{path} is unreadable ({error}); pass --body-bytes"))
}

/// The smallest MTU among the interface's devices less its IP header and the TCP header, the MSS before options.
fn segment_payload(interface: &Interface) -> Result<u64, String> {
    let address = interface.addresses[0];
    let devices = match &interface.device {
        Some(device) => vec![device.clone()],
        None => devices_of(address),
    };
    if devices.is_empty() {
        return Err(format!(
            "no device holds {address}, so its MTU is unknown; pass --body-bytes"
        ));
    }
    let mut mtu = u64::MAX;
    for device in devices {
        let path = format!("/sys/class/net/{device}/mtu");
        let text = read_setting(&path)?;
        mtu = mtu.min(text.trim().parse().map_err(|_| format!("{path} holds no MTU"))?);
    }
    let ip_header = match address {
        IpAddr::V4(_) => IPV4_HEADER_BYTES,
        IpAddr::V6(_) => IPV6_HEADER_BYTES,
    };
    Ok(mtu.saturating_sub(ip_header + TCP_HEADER_BYTES))
}

/// Every device and address in the kernel's interface address list.
#[cfg(target_os = "linux")]
fn interface_addresses() -> Vec<(String, IpAddr)> {
    let mut list: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut list) } != 0 {
        return Vec::new();
    }
    let mut found = Vec::new();
    let mut entry = list;
    // SAFETY: `getifaddrs` returned a list of valid entries, freed only below.
    while let Some(current) = unsafe { entry.as_ref() } {
        if let Some(address) = unsafe { address_of(current.ifa_addr) } {
            let name = unsafe { std::ffi::CStr::from_ptr(current.ifa_name) };
            found.push((name.to_string_lossy().into_owned(), address));
        }
        entry = current.ifa_next;
    }
    unsafe { libc::freeifaddrs(list) };
    found
}

#[cfg(not(target_os = "linux"))]
fn interface_addresses() -> Vec<(String, IpAddr)> {
    Vec::new()
}

/// The devices an address names: the one holding it, or every device holding an address for a wildcard, which
/// listens on all of them.
fn devices_of(address: IpAddr) -> Vec<String> {
    let listed = interface_addresses().into_iter();
    let mut devices: Vec<String> = match address.is_unspecified() {
        true => listed.map(|(device, _)| device).collect(),
        false => listed
            .filter(|&(_, held)| held == address)
            .map(|(device, _)| device)
            .collect(),
    };
    devices.sort_unstable();
    devices.dedup();
    devices
}

/// The IP address a socket address holds, `None` for other families or no address.
///
/// # Safety
/// `socket_address` is null or points to a socket address of the size its family implies.
#[cfg(target_os = "linux")]
unsafe fn address_of(socket_address: *const libc::sockaddr) -> Option<IpAddr> {
    match i32::from(unsafe { socket_address.as_ref() }?.sa_family) {
        libc::AF_INET => {
            let ipv4 = unsafe { &*socket_address.cast::<libc::sockaddr_in>() };
            Some(IpAddr::from(u32::from_be(ipv4.sin_addr.s_addr).to_be_bytes()))
        }
        libc::AF_INET6 => {
            let ipv6 = unsafe { &*socket_address.cast::<libc::sockaddr_in6>() };
            Some(IpAddr::from(ipv6.sin6_addr.s6_addr))
        }
        _ => None,
    }
}

/// A client's flags.
#[derive(clap::Args)]
pub struct ClientArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Server addresses, paired with --interfaces by position.
    #[arg(long, env = "SERVEVAL_TARGETS", value_delimiter = ',', default_value = "127.0.0.1")]
    pub targets: Vec<IpAddr>,
    /// Connection counts to sweep, like 64,10K,1M.
    #[arg(long, env = "SERVEVAL_CONNECTIONS", value_delimiter = ',', default_value = "64", value_parser = parse_count_flag)]
    pub connections: Vec<usize>,
    #[arg(
        long,
        env = "SERVEVAL_WORKLOADS",
        value_enum,
        value_delimiter = ',',
        default_value = "tool,context,download,fanout"
    )]
    pub workloads: Vec<Workload>,
    #[arg(
        long,
        env = "SERVEVAL_VARIANTS",
        value_enum,
        value_delimiter = ',',
        default_value = "plain,batched"
    )]
    pub variants: Vec<Variant>,
    #[arg(
        long,
        env = "SERVEVAL_PHASES",
        value_enum,
        value_delimiter = ',',
        default_value = "steady,handshake"
    )]
    pub phases: Vec<Phase>,
    /// Tool call shapes to sweep: raw bytes, a typed body, or a typed body with path, query and header values.
    #[arg(
        long,
        env = "SERVEVAL_PARSING",
        value_enum,
        value_delimiter = ',',
        default_value = "body"
    )]
    pub parsing: Vec<Parsing>,
    #[command(flatten)]
    pub body_bytes: BodyBytes,
    /// Calls each connection keeps in flight to sweep, or server-maximum for the server's stream limit.
    #[arg(long, env = "SERVEVAL_CALLS_IN_FLIGHT_PER_CONNECTION", value_delimiter = ',', default_value = "1", value_parser = parse_calls_in_flight_flag)]
    pub calls_in_flight_per_connection: Vec<CallsInFlight>,
    /// Scheduled calls per second across this process; unset runs a closed loop.
    #[arg(long, env = "SERVEVAL_CALLS_PER_SECOND", value_parser = parse_count_flag)]
    pub calls_per_second: Option<usize>,
    /// Upstream calls per fan-out.
    #[arg(long, env = "SERVEVAL_WIDTH", default_value = "8", value_parser = parse_count_flag)]
    pub width: usize,
    /// Measured time per configuration, such as 10s or 500ms.
    #[arg(long, env = "SERVEVAL_TIME_LIMIT", default_value = "10s", value_parser = parse_duration_flag)]
    pub time_limit: Duration,
    /// Unmeasured calls before each measured window.
    #[arg(long, env = "SERVEVAL_WARMUP", default_value = "1s", value_parser = parse_duration_flag)]
    pub warmup: Duration,
    /// Client memory budget, at most the cgroup's.
    #[arg(long, env = "SERVEVAL_MEMORY_LIMIT", value_parser = parse_size_flag)]
    pub memory_limit: Option<u64>,
    #[arg(long, env = "SERVEVAL_BETWEEN_RUNS", value_enum, default_value = "reconnect")]
    pub between_runs: BetweenRuns,
    /// Directory for `<framework>-<config-hash>.json` reports.
    #[arg(long, env = "SERVEVAL_OUTPUT", default_value = "results")]
    pub output: PathBuf,
}

/// `--calls-in-flight-per-connection` that reads the server's `max_concurrent_streams_per_connection` from `/health`.
pub const SERVER_MAXIMUM: &str = "server-maximum";
/// RFC 9113 section 5.1.2 recommends that a server's SETTINGS_MAX_CONCURRENT_STREAMS "be no smaller than 100", so a
/// server advertising no limit gets the depth every conforming server allows.
pub const RFC_9113_MINIMUM_CONCURRENT_STREAMS: usize = 100;
/// Why a client that keeps one call in flight per connection cannot run a deeper configuration.
pub const ONE_CALL_PER_CONNECTION: &str = "client keeps one call in flight per connection";
/// Why a configuration the server answers 404, 405 or 501, or gRPC's `UNIMPLEMENTED`, cannot run.
pub const NOT_SERVED: &str = "server does not serve this call";

/// One `--calls-in-flight-per-connection` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallsInFlight {
    Count(NonZeroUsize),
    /// The `max_concurrent_streams_per_connection` the server's `/health` advertises.
    ServerMaximum,
}

impl fmt::Display for CallsInFlight {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(formatter, "{count}"),
            Self::ServerMaximum => formatter.write_str(SERVER_MAXIMUM),
        }
    }
}

/// Where a run's calls in flight per connection came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallsInFlightSource {
    Flag,
    ServerAdvertised,
    /// The server advertises no limit, so the depth is `RFC_9113_MINIMUM_CONCURRENT_STREAMS`.
    Rfc9113Minimum,
}

impl CallsInFlightSource {
    pub fn name(self) -> &'static str {
        match self {
            Self::Flag => "flag",
            Self::ServerAdvertised => "server-advertised",
            Self::Rfc9113Minimum => "rfc9113-minimum",
        }
    }
}

/// The depth one connection carries and where the number came from: the flag, the server's advertised
/// `max_concurrent_streams_per_connection`, or RFC 9113's recommended minimum when it advertises no limit.
pub fn calls_in_flight_per_connection(
    requested: CallsInFlight,
    advertised: Option<u32>,
) -> (usize, CallsInFlightSource) {
    match (requested, advertised) {
        (CallsInFlight::Count(count), _) => (count.get(), CallsInFlightSource::Flag),
        (CallsInFlight::ServerMaximum, None) => {
            (RFC_9113_MINIMUM_CONCURRENT_STREAMS, CallsInFlightSource::Rfc9113Minimum)
        }
        (CallsInFlight::ServerMaximum, Some(0)) => {
            exit("the server advertises 0 concurrent streams per connection, so no call can run")
        }
        (CallsInFlight::ServerMaximum, Some(advertised)) => {
            (advertised as usize, CallsInFlightSource::ServerAdvertised)
        }
    }
}

/// One configuration.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    pub workload: Workload,
    pub variant: Variant,
    /// Swept only for plain tool calls, `body` for every other run.
    pub parsing: Parsing,
    pub phase: Phase,
    pub connections: usize,
    pub calls_in_flight_per_connection: usize,
    pub calls_in_flight_per_connection_source: CallsInFlightSource,
    /// `connections` times `calls_in_flight_per_connection`.
    pub in_flight: usize,
    /// Body size of a context upload or download, 0 for the others.
    pub body_bytes: u64,
    /// Why the client cannot run this configuration, `None` when it can.
    pub unsupported: Option<&'static str>,
}

impl Run {
    /// `throughput` when calls are amortized, by more than one in flight per connection or by server-side batching,
    /// and `latency` otherwise.
    pub fn test(&self) -> &'static str {
        match self.calls_in_flight_per_connection > 1 || self.variant == Variant::Batched {
            true => "throughput",
            false => "latency",
        }
    }
}

/// Why a batched configuration has no measurement: the server ships no batching.
pub const SERVER_DOES_NOT_BATCH: &str = "server does not batch calls";
/// Why a batched upload has no measurement: uploads stream one body per call.
pub const UPLOADS_NOT_BATCHED: &str = "uploads stream one body per call, so they are not batched";
/// Why a batched download has no measurement: downloads stream one body per call.
pub const DOWNLOADS_NOT_BATCHED: &str = "downloads stream one body per call, so they are not batched";

impl ClientArgs {
    /// Every configuration; batched variants only where the server batches, and never for uploads or downloads. A
    /// client that does not multiplex keeps one call in flight per connection, so its deeper configurations are
    /// `unsupported`.
    pub fn runs(&self, batching: bool, multiplexed: bool, advertised: Option<u32>) -> Vec<Run> {
        let depths: Vec<(usize, CallsInFlightSource)> = (self.calls_in_flight_per_connection.iter())
            .map(|&requested| calls_in_flight_per_connection(requested, advertised))
            .collect();
        let mut runs = Vec::new();
        for &workload in &self.workloads {
            let sizes = match workload {
                Workload::Context | Workload::Download => self.body_bytes().to_vec(),
                _ => vec![0],
            };
            for &variant in &self.variants {
                let shapes = match (workload, variant) {
                    (Workload::Tool, Variant::Plain) => self.parsing.clone(),
                    _ => vec![Parsing::Body],
                };
                for &parsing in &shapes {
                    for &phase in &self.phases {
                        for &connections in &self.connections {
                            for &(depth, source) in &depths {
                                let unsupported = match (variant, workload) {
                                    (Variant::Batched, Workload::Context) => Some(UPLOADS_NOT_BATCHED),
                                    (Variant::Batched, Workload::Download) => Some(DOWNLOADS_NOT_BATCHED),
                                    (Variant::Batched, _) if !batching => Some(SERVER_DOES_NOT_BATCH),
                                    _ => (!multiplexed && depth != 1).then_some(ONE_CALL_PER_CONNECTION),
                                };
                                runs.extend(sizes.iter().map(|&body_bytes| Run {
                                    workload,
                                    variant,
                                    parsing,
                                    phase,
                                    connections,
                                    calls_in_flight_per_connection: depth,
                                    calls_in_flight_per_connection_source: source,
                                    in_flight: connections * depth,
                                    body_bytes,
                                    unsupported,
                                }));
                            }
                        }
                    }
                }
            }
        }
        runs
    }

    /// The context upload and download sizes, `BodyBytes::sizes` of this client's interfaces.
    pub fn body_bytes(&self) -> &[u64] {
        self.body_bytes.sizes(&self.common.interfaces)
    }

    /// The local interface paired with target `index`, a single interface serving every target.
    pub fn interface_of(&self, index: usize) -> &Interface {
        let interfaces = &self.common.interfaces;
        &interfaces[index % interfaces.len()]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Security {
    None,
    NoiseXx,
    NoiseIk,
    Tls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Workload {
    Tool,
    /// `PUT /context` with `--body-bytes` seeded bytes, answered with their count.
    Context,
    /// `GET /context?bytes=N`, answered with the first N seeded bytes of the context body.
    Download,
    Fanout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Variant {
    Plain,
    Batched,
}

/// How much of a tool call the server must read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Parsing {
    /// `POST /raw`, answered with the first 256 body bytes unchanged.
    Raw,
    /// `POST /tool`, answered with the id and `after + top_k` from the typed body.
    Body,
    /// `POST /tool/{name}?lang=..&after=..` behind a bearer token and a `traceparent`, answered with all of them.
    Full,
}

/// What happens to a client's connections between two configurations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BetweenRuns {
    /// Reuse the open connections, opening only those a larger count adds.
    Keep,
    /// Close every connection, and open fresh ones for the next configuration.
    Reconnect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Phase {
    /// Calls reuse established connections.
    Steady,
    /// Every call opens, and for secure modes handshakes, its own connection.
    Handshake,
}

/// Spells a value the way the command line takes it.
pub fn spell(value: &impl ValueEnum) -> String {
    value
        .to_possible_value()
        .map_or_else(String::new, |value| value.get_name().to_owned())
}

/// A local device and its addresses; an empty device routes by address alone.
#[derive(Clone, Debug)]
pub struct Interface {
    pub device: Option<String>,
    pub addresses: Vec<IpAddr>,
}

impl std::fmt::Display for Interface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        let addresses: Vec<String> = self.addresses.iter().map(IpAddr::to_string).collect();
        match &self.device {
            Some(device) => write!(formatter, "{device}={}", addresses.join("+")),
            None => write!(formatter, "{}", addresses.join("+")),
        }
    }
}

/// Exits if any interface names a device, for frameworks that bind by address alone.
pub fn require_no_device(common: &CommonArgs, framework: &str) {
    if common.interfaces.iter().any(|interface| interface.device.is_some()) {
        exit(&format!(
            "{framework} binds by address alone; list interfaces as ADDRESS"
        ));
    }
}

/// Opens a listener on the interface's first address, bound to its device, with `backlog` pending connections.
pub fn listen(interface: &Interface, port: u16, backlog: usize) -> io::Result<std::net::TcpListener> {
    let address = SocketAddr::new(interface.addresses[0], port);
    let socket = Socket::new(Domain::for_address(address), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    // Accepted connections inherit it, so no framework waits on Nagle's algorithm.
    socket.set_tcp_nodelay(true)?;
    bind_device(&socket, interface)?;
    socket.bind(&address.into())?;
    socket.listen(i32::try_from(backlog).unwrap_or(i32::MAX))?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Connects to `target` through the interface's device, from its addresses in turn by `index`.
pub async fn connect(interface: &Interface, index: usize, target: SocketAddr) -> io::Result<tokio::net::TcpStream> {
    let socket = Socket::new(Domain::for_address(target), Type::STREAM, Some(Protocol::TCP))?;
    bind_device(&socket, interface)?;
    defer_port_choice(&socket)?;
    let source = interface.addresses[index % interface.addresses.len()];
    socket.bind(&SocketAddr::new(source, 0).into())?;
    socket.set_tcp_nodelay(true)?;
    socket.set_nonblocking(true)?;
    tokio::net::TcpSocket::from_std_stream(socket.into())
        .connect(target)
        .await
}

fn bind_device(
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] socket: &Socket,
    interface: &Interface,
) -> io::Result<()> {
    match &interface.device {
        None => Ok(()),
        #[cfg(target_os = "linux")]
        Some(device) => socket.bind_device(Some(device.as_bytes())),
        #[cfg(not(target_os = "linux"))]
        Some(device) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("binding device {device} needs Linux"),
        )),
    }
}

/// Lets `connect` pick the source port, so one source address reaches past 64K ports across several targets.
#[cfg(target_os = "linux")]
fn defer_port_choice(socket: &Socket) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let enabled: libc::c_int = 1;
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_BIND_ADDRESS_NO_PORT,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn defer_port_choice(_: &Socket) -> io::Result<()> {
    Ok(())
}

/// Resolves on SIGINT or SIGTERM, which systemd sends to stop a unit.
pub async fn stop_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let (Ok(mut interrupt), Ok(mut terminate)) = (signal(SignalKind::interrupt()), signal(SignalKind::terminate()))
    else {
        return std::future::pending().await;
    };
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
    }
}

/// This process's place among the clients launch.py starts, from `SERVEVAL_PROCESS` as `INDEX/COUNT`.
pub fn process() -> (usize, usize) {
    let text = std::env::var("SERVEVAL_PROCESS").unwrap_or_default();
    let parsed = text
        .split_once('/')
        .and_then(|(index, count)| Some((index.parse().ok()?, count.parse().ok()?)));
    match parsed {
        Some((index, count)) if index < count => (index, count),
        None if text.is_empty() => (0, 1),
        _ => exit(&format!(
            "SERVEVAL_PROCESS=\"{text}\" does not parse, expected INDEX/COUNT like 0/2"
        )),
    }
}

/// How long after the last client finished connecting the window opens.
const BARRIER_DELAY: Duration = Duration::from_millis(100);

/// Run `run` starts 100ms after the last launched client finished connecting for it: each client writes its Unix
/// nanoseconds to `<SERVEVAL_BARRIER>/<run>-<process>` and waits for the files of every process, or starts now
/// without the variable.
pub fn start_at(run: usize) -> Instant {
    let Ok(directory) = std::env::var("SERVEVAL_BARRIER") else {
        return Instant::now();
    };
    let (index, count) = process();
    let directory = Path::new(&directory);
    let unix = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let mine = directory.join(format!("{run}-{index}"));
    let staged = mine.with_extension("tmp");
    let written = std::fs::write(&staged, unix().as_nanos().to_string()).and_then(|()| std::fs::rename(&staged, &mine));
    if let Err(error) = written {
        exit(&format!(
            "SERVEVAL_BARRIER=\"{}\" is not writable: {error}",
            directory.display()
        ));
    }
    let latest = loop {
        let read = (0..count).map(|other| std::fs::read_to_string(directory.join(format!("{run}-{other}"))).ok());
        if let Some(stamps) = read.collect::<Option<Vec<String>>>() {
            break stamps
                .iter()
                .filter_map(|stamp| stamp.parse::<u64>().ok())
                .max()
                .unwrap_or_default();
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let start = Duration::from_nanos(latest) + BARRIER_DELAY;
    Instant::now() + start.saturating_sub(unix())
}

/// Part `index` of `total` split evenly over `parts`, the first parts taking the remainder.
pub fn share(total: usize, index: usize, parts: usize) -> usize {
    total / parts + usize::from(index < total % parts)
}

/// Prints `message` and exits with 1.
pub fn exit(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}

/// Parses the command line; a bad value prints `--name="value" does not parse, expected ...` and exits with 1.
pub fn parse_cli<T: clap::Parser>() -> T {
    use clap::error::{ContextKind, ContextValue, ErrorKind};

    let error = match T::try_parse() {
        Ok(cli) => return cli,
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => error,
    };
    let context = |kind| match error.get(kind) {
        Some(ContextValue::String(text)) => text.as_str(),
        _ => "",
    };
    let expected = match (
        error.kind(),
        std::error::Error::source(&error),
        error.get(ContextKind::ValidValue),
    ) {
        (ErrorKind::ValueValidation, Some(expected), _) => expected.to_string(),
        (ErrorKind::InvalidValue, _, Some(ContextValue::Strings(values))) if !values.is_empty() => {
            format!("expected one of {}", values.join(", "))
        }
        (ErrorKind::InvalidValue, _, _) => "expected a non-empty value".into(),
        _ => {
            eprint!("{error}");
            std::process::exit(1)
        }
    };
    let flag = context(ContextKind::InvalidArg).split(' ').next().unwrap_or_default();
    eprintln!(
        "{flag}=\"{}\" does not parse, {expected}",
        context(ContextKind::InvalidValue)
    );
    std::process::exit(1)
}

/// Parses a 32-bit unsigned integer, or `random` as 32 bits from the OS entropy source.
pub fn parse_seed(text: &str) -> Option<Seed> {
    if text == "random" {
        return Some(Seed(std::hash::RandomState::new().build_hasher().finish() as u32));
    }
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok().map(Seed)).flatten()
}

/// Parses a thread count like `8`, or `0` as `all_cores`.
pub fn parse_threads(text: &str, all_cores: NonZeroUsize) -> Option<Threads> {
    match text {
        "0" => Some(Threads(all_cores)),
        _ => parse_count(text).and_then(NonZeroUsize::new).map(Threads),
    }
}

/// Parses a count like `10000` or `10M`: `K`, `M`, `G`, `T` are powers of 1000 in any case, and zero is `None`.
pub fn parse_count(text: &str) -> Option<usize> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let exponent = ["", "k", "m", "g", "t"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit))?;
    let count = text[..split]
        .parse::<usize>()
        .ok()?
        .checked_mul(1000usize.pow(exponent as u32))?;
    (count != 0).then_some(count)
}

fn parse_digits(text: &str) -> Option<u64> {
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| text.parse().ok()).flatten().filter(|&count| count != 0)
}

/// Parses a duration like `200ms` or `10s`; a bare number, a fraction or zero is `None`.
pub fn parse_duration(text: &str) -> Option<Duration> {
    match text.strip_suffix("ms") {
        Some(count) => parse_digits(count).map(Duration::from_millis),
        None => parse_digits(text.strip_suffix('s')?).map(Duration::from_secs),
    }
}

/// Parses a size like `4096`, `64KB` or `16GB`, in powers of 1024; zero is `None`.
pub fn parse_size(text: &str) -> Option<u64> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let level = ["", "kb", "mb", "gb", "tb"]
        .iter()
        .position(|unit| text[split..].eq_ignore_ascii_case(unit))?;
    let bytes = text[..split].parse::<u64>().ok()?.checked_mul(1 << (10 * level))?;
    (bytes != 0).then_some(bytes)
}

/// Spells a size the way `parse_size` reads it: `16GB`, `1000`.
pub fn spell_size(bytes: u64) -> String {
    let (mut count, mut unit) = (bytes, "");
    for larger in ["KB", "MB", "GB", "TB"] {
        if count == 0 || count % 1024 != 0 {
            break;
        }
        count /= 1024;
        unit = larger;
    }
    format!("{count}{unit}")
}

/// Spells a duration the way `parse_duration` reads it: `1s`, `1500ms`.
pub fn spell_duration(duration: Duration) -> String {
    let milliseconds = duration.as_millis();
    match milliseconds % 1000 {
        0 => format!("{}s", milliseconds / 1000),
        _ => format!("{milliseconds}ms"),
    }
}

/// A 32-bit run seed, an integer or drawn from the OS for `random`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seed(pub u32);

impl From<Seed> for u64 {
    fn from(seed: Seed) -> u64 {
        u64::from(seed.0)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A thread count; `0` in the variable resolves to every core when read, so it is never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Threads(pub NonZeroUsize);

impl Threads {
    /// Every core in this process's affinity mask, at least one.
    pub fn all_cores() -> Self {
        Self(std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }
}

impl fmt::Display for Threads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Parses `DEVICE=ADDRESS[+ADDRESS...]` or `ADDRESS[+ADDRESS...]`.
pub fn parse_interface(text: &str) -> Option<Interface> {
    let (device, addresses) = match text.split_once('=') {
        Some((device, addresses)) if !device.is_empty() => (Some(device.to_owned()), addresses),
        Some(_) => return None,
        None => (None, text),
    };
    let addresses = addresses
        .split('+')
        .map(|address| address.parse().ok())
        .collect::<Option<Vec<IpAddr>>>()?;
    Some(Interface { device, addresses })
}

/// `parse_count` for clap.
pub fn parse_count_flag(text: &str) -> Result<usize, String> {
    parse_count(text).ok_or_else(|| "expected a positive count like 1000 or 10M".into())
}

/// Parses a positive count of calls each connection keeps in flight, or `server-maximum`.
pub fn parse_calls_in_flight(text: &str) -> Option<CallsInFlight> {
    match text {
        SERVER_MAXIMUM => Some(CallsInFlight::ServerMaximum),
        _ => parse_digits(text)
            .and_then(|count| NonZeroUsize::new(usize::try_from(count).ok()?))
            .map(CallsInFlight::Count),
    }
}

/// `parse_calls_in_flight` for clap.
pub fn parse_calls_in_flight_flag(text: &str) -> Result<CallsInFlight, String> {
    parse_calls_in_flight(text).ok_or_else(|| format!("expected positive integers or {SERVER_MAXIMUM}"))
}

/// `parse_threads` for clap, with `0` resolved to every available core.
pub fn parse_threads_flag(text: &str) -> Result<Threads, String> {
    parse_threads(text, Threads::all_cores().0).ok_or_else(|| "expected a count, 0 for all cores".into())
}

/// `parse_size` for clap.
pub fn parse_size_flag(text: &str) -> Result<u64, String> {
    parse_size(text).ok_or_else(|| "expected a size like 4096, 64KB or 16GB".into())
}

/// `parse_seed` for clap.
pub fn parse_seed_flag(text: &str) -> Result<Seed, String> {
    parse_seed(text).ok_or_else(|| "expected an unsigned integer or random".into())
}

/// `parse_duration` for clap.
pub fn parse_duration_flag(text: &str) -> Result<Duration, String> {
    parse_duration(text).ok_or_else(|| "expected a duration like 200ms or 10s".into())
}

/// `parse_digits` for clap, as a TCP port from 1 to 65535.
pub fn parse_port_flag(text: &str) -> Result<u16, String> {
    parse_digits(text)
        .and_then(|port| u16::try_from(port).ok())
        .ok_or_else(|| "expected a port from 1 to 65535".into())
}

/// `parse_interface` for clap.
pub fn parse_interface_flag(text: &str) -> Result<Interface, String> {
    parse_interface(text).ok_or_else(|| "expected DEVICE=ADDRESS or ADDRESS, extra addresses joined by +".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar() {
        assert_eq!(parse_count("10K"), Some(10_000));
        assert_eq!(parse_count("0"), None);
        assert_eq!(parse_size("64KB"), Some(65_536));
        assert_eq!(parse_duration("200ms"), Some(Duration::from_millis(200)));
        assert_eq!(parse_duration("2m"), None);
        assert_eq!(
            parse_calls_in_flight("server-maximum"),
            Some(CallsInFlight::ServerMaximum)
        );
        assert_eq!(parse_calls_in_flight("0"), None);
        assert_eq!(
            calls_in_flight_per_connection(CallsInFlight::ServerMaximum, None),
            (100, CallsInFlightSource::Rfc9113Minimum)
        );
        let interface = parse_interface("ens5=10.0.1.16+10.0.1.17").unwrap();
        assert_eq!(interface.device.as_deref(), Some("ens5"));
        assert_eq!(interface.addresses.len(), 2);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn default_body_bytes_spans_one_segment_to_the_receive_buffer() {
        let loopback = parse_interface("127.0.0.1").unwrap();
        let sizes = default_body_bytes(&[loopback]).unwrap();
        let mtu: u64 = std::fs::read_to_string("/sys/class/net/lo/mtu")
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let timestamps = std::fs::read_to_string("/proc/sys/net/ipv4/tcp_timestamps")
            .unwrap()
            .trim()
            != "0";
        let options = if timestamps { TCP_TIMESTAMPS_OPTION_BYTES } else { 0 };
        assert_eq!(sizes[0], mtu - IPV4_HEADER_BYTES - TCP_HEADER_BYTES - options);
        assert!(sizes.contains(&EARLIER_UPLOAD_BYTES));
        let powers: Vec<u64> = (sizes.iter().copied())
            .filter(|&size| size == sizes[0] || (size % sizes[0] == 0 && (size / sizes[0]).is_power_of_two()))
            .collect();
        assert!(powers.windows(2).all(|pair| pair[1] == pair[0] * 4));
        let receive_buffer = std::fs::read_to_string("/proc/sys/net/ipv4/tcp_rmem").unwrap();
        let largest: u64 = receive_buffer.split_whitespace().nth(2).unwrap().parse().unwrap();
        let last = *powers.last().unwrap();
        assert!(powers.len() == 1 || (last <= largest && last * 4 > largest));
    }
}
