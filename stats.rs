//! Latency histograms that merge exactly across processes, the call schedule, and cgroup and process counters.

use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SUB_BINS: usize = 16;
/// Bits of the largest value with an exact bin: every latency below 32 ns gets one.
const EXACT_BITS: u32 = 5;
const BINS: usize = (64 - EXACT_BITS as usize + 2) * SUB_BINS;

/// Nanosecond latencies in 16 linear bins per power of two, as UCall's driver keeps them: percentiles read at most
/// 1/16 high, and the same bins in `stats.py` let reports from every language merge.
#[derive(Clone)]
pub struct Histogram {
    pub bins: Vec<u64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Self { bins: vec![0; BINS] }
    }
}

impl Histogram {
    pub fn bin_of(nanoseconds: u64) -> usize {
        if nanoseconds < 2 * SUB_BINS as u64 {
            return nanoseconds as usize;
        }
        let shift = 64 - nanoseconds.leading_zeros() - EXACT_BITS;
        shift as usize * SUB_BINS + (nanoseconds >> shift) as usize
    }

    pub fn upper_edge(bin: usize) -> u64 {
        if bin < 2 * SUB_BINS {
            return bin as u64;
        }
        let shift = bin / SUB_BINS - 1;
        let leading = (bin % SUB_BINS + SUB_BINS) as u64;
        ((leading + 1) << shift).wrapping_sub(1)
    }

    pub fn record(&mut self, latency: Duration) {
        self.bins[Self::bin_of(latency.as_nanos().min(u128::from(u64::MAX)) as u64)] += 1;
    }

    pub fn merge(&mut self, other: &Self) {
        self.bins
            .iter_mut()
            .zip(&other.bins)
            .for_each(|(bin, other)| *bin += other);
    }

    /// The upper edge in nanoseconds of the bin holding the nearest-rank `fraction`, like 0.999 for p99.9.
    pub fn percentile(&self, fraction: f64) -> u64 {
        let total: u64 = self.bins.iter().sum();
        if total == 0 {
            return 0;
        }
        let rank = ((fraction * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0;
        let bin = self.bins.iter().position(|&count| {
            cumulative += count;
            cumulative >= rank
        });
        Self::upper_edge(bin.unwrap_or(BINS - 1))
    }

    /// The non-empty bins as `[bin, count]` pairs.
    pub fn sparse(&self) -> Vec<[u64; 2]> {
        let bins = self.bins.iter().enumerate().filter(|(_, &count)| count != 0);
        bins.map(|(bin, &count)| [bin as u64, count]).collect()
    }
}

/// Why a call failed.
#[derive(Clone, Copy, Debug)]
pub enum Failure {
    /// Any transport failure but a refusal or a timeout, while opening a connection or on an open one.
    Connect,
    Refused,
    Timeout,
    /// A status other than success.
    Status,
    /// A successful status with a reply that differs from the expected one.
    WrongReply,
}

impl Failure {
    pub fn of_io(error: &io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::ConnectionRefused => Self::Refused,
            io::ErrorKind::TimedOut => Self::Timeout,
            _ => Self::Connect,
        }
    }

    /// The class of the first I/O error in `error`'s chain of sources, `Connect` without one.
    pub fn of(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut source = Some(error);
        while let Some(error) = source {
            if let Some(error) = error.downcast_ref::<io::Error>() {
                return Self::of_io(error);
            }
            source = error.source();
        }
        Self::Connect
    }
}

/// Failed calls by `Failure`.
#[derive(Clone, Copy, Default, serde::Serialize)]
pub struct Failures {
    pub connect: u64,
    pub refused: u64,
    pub timeout: u64,
    pub status: u64,
    pub wrong_reply: u64,
}

impl Failures {
    pub fn count(&mut self, failure: Failure) {
        *match failure {
            Failure::Connect => &mut self.connect,
            Failure::Refused => &mut self.refused,
            Failure::Timeout => &mut self.timeout,
            Failure::Status => &mut self.status,
            Failure::WrongReply => &mut self.wrong_reply,
        } += 1;
    }

    pub fn total(&self) -> u64 {
        self.connect + self.refused + self.timeout + self.status + self.wrong_reply
    }

    pub fn merge(&mut self, other: &Self) {
        self.connect += other.connect;
        self.refused += other.refused;
        self.timeout += other.timeout;
        self.status += other.status;
        self.wrong_reply += other.wrong_reply;
    }
}

/// What a set of calls returned.
#[derive(Clone, Default)]
pub struct Tally {
    pub latencies: Histogram,
    /// How late each measured call left against its schedule.
    pub lateness: Histogram,
    /// Valid replies inside the measured window.
    pub calls: u64,
    /// Failed or mismatched replies inside the measured window.
    pub failures: Failures,
    /// Measured calls the schedule owed that the window closed on before they left.
    pub unsent: u64,
    /// Every call, warmup included, which the server's CPU time covers.
    pub all_calls: u64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
}

impl Tally {
    pub fn merge(&mut self, other: &Self) {
        self.latencies.merge(&other.latencies);
        self.lateness.merge(&other.lateness);
        self.calls += other.calls;
        self.failures.merge(&other.failures);
        self.unsent += other.unsent;
        self.all_calls += other.all_calls;
        self.request_body_bytes += other.request_body_bytes;
        self.response_body_bytes += other.response_body_bytes;
    }
}

/// When calls go out: a warmup, then the measured window, closed-loop or on one global arrival schedule.
#[derive(Clone, Copy)]
pub struct Window {
    pub begin: Instant,
    pub measured_from: Instant,
    pub until: Instant,
    pub calls_per_second: Option<usize>,
    /// Slots sharing the schedule, each with one call outstanding.
    pub slots: usize,
}

impl Window {
    pub fn new(
        begin: Instant,
        warmup: Duration,
        time_limit: Duration,
        calls_per_second: Option<usize>,
        slots: usize,
    ) -> Self {
        let measured_from = begin + warmup;
        Self {
            begin,
            measured_from,
            until: measured_from + time_limit,
            calls_per_second,
            slots,
        }
    }

    pub fn seconds(&self) -> f64 {
        (self.until - self.measured_from).as_secs_f64()
    }

    /// The measured window's first and last moment in Unix milliseconds, to find it in other recordings.
    pub fn unix_ms(&self) -> [u64; 2] {
        let (unix, now) = (SystemTime::now(), Instant::now());
        [self.measured_from, self.until].map(|moment| {
            let wall = match moment.checked_duration_since(now) {
                Some(ahead) => unix + ahead,
                None => unix - (now - moment),
            };
            wall.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
        })
    }

    /// When open-loop call `sequence` is due, `None` in a closed loop.
    fn scheduled(&self, sequence: usize) -> Option<Instant> {
        let rate = self.calls_per_second?;
        Some(self.begin + Duration::from_secs_f64(sequence as f64 / rate as f64))
    }
}

/// One call outstanding at a time on the window's schedule: `next` waits until a call is due, `record` keeps its
/// outcome. Open-loop latency counts from the intended send, so a stalled server cannot hide its queue, and the calls
/// a late slot never sent count as unsent, so a slow client cannot pass for a fast server.
pub struct Slot {
    window: Window,
    slot: usize,
    arrival: usize,
    pub tally: Tally,
}

impl Slot {
    pub fn new(window: Window, slot: usize) -> Self {
        Self {
            window,
            slot,
            arrival: 0,
            tally: Tally::default(),
        }
    }

    /// The intended send time and sequence number of the next call, or `None` once the window closed.
    pub async fn next(&mut self) -> Option<(Instant, usize)> {
        let window = self.window;
        if self.arrival == 0 {
            tokio::time::sleep_until(window.begin.into()).await;
        }
        let sequence = self.slot + self.arrival * window.slots;
        self.arrival += 1;
        let intended = window.scheduled(sequence).unwrap_or_else(Instant::now);
        if intended >= window.until || Instant::now() >= window.until {
            let owed = (sequence..)
                .step_by(window.slots)
                .map_while(|late| window.scheduled(late));
            let owed = owed.take_while(|&late| late < window.until);
            self.tally.unsent += owed.filter(|&late| late >= window.measured_from).count() as u64;
            return None;
        }
        if window.calls_per_second.is_some() {
            tokio::time::sleep_until(intended.into()).await;
        }
        if intended >= window.measured_from {
            self.tally.lateness.record(intended.elapsed());
        }
        Some((intended, sequence))
    }

    /// Keeps a call's outcome: the body bytes sent and received, or why it failed.
    pub fn record(&mut self, intended: Instant, outcome: Result<(u64, u64), Failure>) {
        let tally = &mut self.tally;
        tally.all_calls += 1;
        if intended < self.window.measured_from {
            return;
        }
        match outcome {
            Ok((sent, received)) => {
                tally.calls += 1;
                tally.request_body_bytes += sent;
                tally.response_body_bytes += received;
                tally.latencies.record(intended.elapsed());
            }
            Err(failure) => tally.failures.count(failure),
        }
    }
}

/// A process's cgroup v2 counters, which cover every worker process and the kernel's socket memory.
#[derive(Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct UnitStats {
    pub cpu_microseconds: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub cores: u64,
    pub memory_limit: Option<u64>,
    /// The box's `tcp_active_opens`, not the unit's.
    pub tcp_active_opens: Option<u64>,
}

impl UnitStats {
    /// The counters of several servers together; a counter missing from any of them is missing.
    pub fn sum(all: &[UnitStats]) -> Self {
        let total = |field: fn(&UnitStats) -> Option<u64>| all.iter().map(field).sum::<Option<u64>>();
        UnitStats {
            cpu_microseconds: total(|stats| stats.cpu_microseconds),
            memory_bytes: total(|stats| stats.memory_bytes),
            cores: all.iter().map(|stats| stats.cores).sum(),
            memory_limit: total(|stats| stats.memory_limit),
            // Every target on one box reads the same counter.
            tcp_active_opens: all.iter().map(|stats| stats.tcp_active_opens).max().flatten(),
        }
    }
}

/// This process's cgroup counters; `None` where the platform has no cgroup v2.
pub fn unit_stats() -> UnitStats {
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get() as u64);
    let Some(directory) = cgroup_directory() else {
        return UnitStats {
            cores,
            tcp_active_opens: tcp_active_opens(),
            ..UnitStats::default()
        };
    };
    let read = |name: &str| std::fs::read_to_string(directory.join(name)).ok();
    let cpu_microseconds = read("cpu.stat").and_then(|stat| {
        stat.lines()
            .find_map(|line| line.strip_prefix("usage_usec "))
            .and_then(|value| value.trim().parse().ok())
    });
    let memory_bytes = read("memory.current").and_then(|value| value.trim().parse().ok());
    let memory_limit = directory
        .ancestors()
        .take_while(|ancestor| ancestor.starts_with("/sys/fs/cgroup"))
        .filter_map(|ancestor| {
            std::fs::read_to_string(ancestor.join("memory.max"))
                .ok()?
                .trim()
                .parse()
                .ok()
        })
        .min();
    UnitStats {
        cpu_microseconds,
        memory_bytes,
        cores,
        memory_limit,
        tcp_active_opens: tcp_active_opens(),
    }
}

/// TCP connections this box opened since boot, every process together, from `/proc/net/snmp`.
pub fn tcp_active_opens() -> Option<u64> {
    let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut tcp = snmp.lines().filter_map(|line| line.strip_prefix("Tcp:"));
    let (names, values) = (tcp.next()?, tcp.next()?);
    let position = names.split_whitespace().position(|name| name == "ActiveOpens")?;
    values.split_whitespace().nth(position)?.parse().ok()
}

/// The box's TCP sockets in use and orphaned, from `/proc/net/sockstat`.
pub fn sockets_in_use() -> Option<[u64; 2]> {
    let sockstat = std::fs::read_to_string("/proc/net/sockstat").ok()?;
    let fields: Vec<&str> = sockstat
        .lines()
        .find_map(|line| line.strip_prefix("TCP:"))?
        .split_whitespace()
        .collect();
    let read = |name: &str| {
        fields
            .iter()
            .position(|&field| field == name)
            .and_then(|at| fields.get(at + 1)?.parse().ok())
    };
    Some([read("inuse")?, read("orphan")?])
}

/// The box's sockets and a server's memory, sampled until closed connections are released.
pub type Release = (Option<[u64; 2]>, Option<u64>);
/// Pause between two `Release` samples.
pub const SETTLE_INTERVAL: Duration = Duration::from_millis(100);
/// Most `Release` samples one reconnect waits for.
pub const SETTLE_ROUNDS: usize = 100;

/// Whether nothing moved between two samples: the same sockets, and memory within 1%.
pub fn settled(earlier: Release, later: Release) -> bool {
    let memory = match (earlier.1, later.1) {
        (Some(earlier), Some(later)) => earlier.abs_diff(later) * 100 <= earlier,
        (earlier, later) => earlier == later,
    };
    earlier.0 == later.0 && memory
}

/// The `/stats` body.
pub fn unit_json() -> String {
    serde_json::to_string(&unit_stats()).unwrap_or_default()
}

fn cgroup_directory() -> Option<PathBuf> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = membership.lines().find_map(|line| line.strip_prefix("0::"))?;
    Some(PathBuf::from("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
}

/// User and system CPU time this process has used.
pub fn process_cpu() -> Duration {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let time = |value: libc::timeval| Duration::new(value.tv_sec as u64, value.tv_usec as u32 * 1000);
    time(usage.ru_utime) + time(usage.ru_stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_cover_every_latency() {
        assert_eq!(Histogram::bin_of(31), 31);
        assert_eq!(Histogram::bin_of(32), 32);
        assert_eq!(Histogram::bin_of(u64::MAX), BINS - 1);
        assert_eq!(Histogram::upper_edge(BINS - 1), u64::MAX);
        for nanoseconds in [1, 100, 1_000_000, 1 << 40] {
            let edge = Histogram::upper_edge(Histogram::bin_of(nanoseconds));
            assert!(edge >= nanoseconds && edge - nanoseconds <= nanoseconds / 16);
        }
    }

    #[tokio::test]
    async fn late_slots_count_the_measured_calls_they_owed() {
        let begin = Instant::now() - Duration::from_secs(1);
        let window = Window::new(
            begin,
            Duration::from_millis(20),
            Duration::from_millis(80),
            Some(100),
            2,
        );
        let mut slot = Slot::new(window, 1);
        assert!(slot.next().await.is_none());
        assert_eq!(slot.tally.unsent, 4, "calls due at 30, 50, 70 and 90ms");
    }
}
