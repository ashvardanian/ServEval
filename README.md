# ServEval

![ServEval benchmarks thumbnail](https://github.com/ashvardanian/ashvardanian/raw/master/repositories/ServEval.jpg?raw=true)

__ServEval__ benchmarks serving frameworks under agentic-scale traffic, from one loopback to multi-NIC pairs.
It is a sibling of [RetriEval](https://github.com/ashvardanian/RetriEval), [CrudEval](https://github.com/ashvardanian/CrudEval) and [TablEval](https://github.com/ashvardanian/TablEval).
It compares [UCall](https://github.com/unum-cloud/UCall), [axum](https://github.com/tokio-rs/axum), [actix-web](https://github.com/actix/actix-web), [tonic](https://github.com/hyperium/tonic), [FastAPI](https://github.com/fastapi/fastapi), [LitServe](https://github.com/Lightning-AI/LitServe), [Ray Serve](https://github.com/ray-project/ray) and [gRPC](https://github.com/grpc/grpc).
Each framework runs with its own client against trivial handlers whose replies are verified byte for byte, and a row is void when its client left calls unsent or an AWS throttle counter rose.

## Quickstart

Loopback on one box, with axum and hyper:

```sh
cargo build --release
export SERVEVAL_INTERFACES=127.0.0.1 SERVEVAL_TARGETS=127.0.0.1 SERVEVAL_TIME_LIMIT=10s
taskset -c 16-19 ./launch.py --copies 8 --port 9001 -- target/release/serv-eval-axum-server &
SERVEVAL_UPSTREAMS=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006,127.0.0.1:9007,127.0.0.1:9008 \
    taskset -c 0-7 ./launch.py -- target/release/serv-eval-axum-server &
taskset -c 8-15 ./launch.py -- target/release/serv-eval-hyper-client --workloads tool,context,fanout --parsing raw,body,full --connections 64,10K
```

Python frameworks run from their own dependency groups, so one framework's pins never break another's:

```sh
uv run --group fastapi ./launch.py -- python fastapi_server.py &
uv run --group httpx ./launch.py -- python httpx_client.py
```

Two boxes with two NICs each, axum over TLS, starting with the server box:

```sh
export SERVEVAL_INTERFACES=ens5=10.0.1.5,ens6=10.0.2.5 SERVEVAL_SECURITY=tls SERVEVAL_CONNECTIONS=1M
./keys.sh
sudo -E ./launch.py --layout per-domain -- target/release/serv-eval-axum-server
```

Then the client box, with the CA from the server box's `data/tls/ca.pem`:

```sh
export SERVEVAL_INTERFACES=ens5=10.0.1.16+10.0.1.17+10.0.1.18+10.0.1.19,ens6=10.0.2.16+10.0.2.17+10.0.2.18+10.0.2.19 \
       SERVEVAL_TARGETS=10.0.1.5,10.0.2.5 SERVEVAL_SECURITY=tls
sudo -E ./launch.py --layout per-domain -- target/release/serv-eval-hyper-client --workloads tool,fanout --connections 64,10K,1M
```

## Backends

| Framework | Language | Server               | Client             | Batching       | Security          |
| :-------- | :------- | :------------------- | :----------------- | :------------- | :---------------- |
| UCall     | Rust     | `ucall_server.rs`    | `ucall_client.rs`  | `/tool/batch`  | Noise IK and XX   |
| UCall     | Python   | `ucall_server.py`    | `ucall_client.py`  | `/tool/batch`  | Noise IK and XX   |
| axum      | Rust     | `axum_server.rs`     | `hyper_client.rs`  | none           | rustls TLS 1.3    |
| actix-web | Rust     | `actix_server.rs`    | `awc_client.rs`    | none           | rustls TLS 1.3    |
| tonic     | Rust     | `tonic_server.rs`    | `tonic_client.rs`  | none           | rustls TLS 1.3    |
| FastAPI   | Python   | `fastapi_server.py`  | `httpx_client.py`  | none           | OpenSSL TLS 1.3   |
| LitServe  | Python   | `litserve_server.py` | `httpx_client.py`  | `/tool/batch`  | OpenSSL TLS 1.3   |
| Ray Serve | Python   | `rayserve_server.py` | `httpx_client.py`  | `@serve.batch` | OpenSSL TLS 1.3   |
| grpc.aio  | Python   | `grpc_server.py`     | `grpc_client.py`   | none           | BoringSSL TLS 1.3 |

Each Rust source builds the binary `serv-eval-` plus its name with dashes, like `serv-eval-axum-server` from `axum_server.rs`, and each client is named for the library it uses.
The two UCall pairs fetch UCall at a pinned commit and write the same reports; UCall's own C++ benchmarks live in UCall.
The Rust pair builds behind the `ucall` feature, on by default, and the Python pair runs on free-threaded CPython with `uv run --python 3.14t --group ucall`.
`httpx_client.py` serves FastAPI, LitServe and Ray Serve, reading the framework from `/health`.
The Rust pairs build behind the `axum`, `actix` and `tonic` features, all on by default; `cargo build --release --no-default-features --features axum` builds one.

## Workloads

Tool calls come in three request shapes, swept by `--parsing raw,body,full` and defaulting to `body`:

- `raw`: `POST /raw` with 1024 bytes, answered with the first 256 body bytes unchanged, which isolates the transport.
- `body`: `POST /tool` with exactly 1024 bytes of JSON, answered with exactly 256 bytes copying the call's `id` and `"check": after + top_k`, so every server must parse typed values.
- `full`: `POST /tool/{name}?lang=en&after=N` with a bearer `authorization` and a W3C `traceparent`, answered with the `id`, the tool name, the trace id, `lang` and `check`.
  A wrong token gets 401 and a `traceparent` that does not parse gets 400.

```text
POST /tool HTTP/1.1
{"id":"call_…","type":"function","name":"search_documents","arguments":{"query":"…","top_k":37,"filters":{"lang":"en","after":1767225600}},"trace":"…"}
→ {"id":"call_…","ok":true,"check":1767225637,"content":"xx…"}

POST /tool/search_documents?lang=en&after=1767225600 HTTP/1.1
authorization: Bearer 6f1c09a4e2b7d35c81f0aa4e92d1c7b3
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
{"id":"call_…","query":"…","top_k":37,"trace":"…"}
→ {"id":"call_…","name":"search_documents","trace":"4bf92f3577b34da6a3ce929d0e0e4736","lang":"en","check":1767225637,"content":"xx…"}
```

Clients compare every reply byte for byte with the one precomputed for its ring entry.
Only plain tool calls sweep the shapes; context uploads, fan-out and the batched variants always send `body`.
LitServe hands its API only the decoded body, so it supports `body` alone and reports `unsupported` for `raw` and `full`.

Context uploads: `PUT /context` with `--body-bytes` seeded bytes, answered with the byte count as 16 hexadecimal digits.
Context downloads: `GET /context?bytes=N`, answered with the first N of the same seeded bytes, from a buffer each server process prepares once, on its first download, for its largest `--body-bytes`.
A download past that buffer gets 416, or `OUT_OF_RANGE` over gRPC, so servers and clients take the same sweep: a `launch.py` pair resolves the client's and hands it to both as `SERVEVAL_BODY_BYTES`.
HTTP servers send the slice as one body the framework writes out as the socket drains; gRPC servers stream it as `Chunk` messages, which uploads stream in too.
Each `Chunk` holds 4MB less 5 bytes: gRPC core's `GRPC_DEFAULT_MAX_RECV_MESSAGE_LENGTH` and tonic's `DEFAULT_MAX_RECV_MESSAGE_SIZE` are both 4MB, and the `data` field's tag and length varint take 1 and 4 of them, so a body crosses in as few messages as every default receiver takes.
hyper, awc and tonic compare the reply chunk by chunk with the bytes they expect, which checks its length and every byte without gathering it.
The UCall clients buffer the reply, capped at the largest download plus the head of the servers' reply to it, measured once from a streamed download of that size, and compare it whole, as the Python clients do.
Uploads and downloads share the `--body-bytes` sweep.
Unset, it runs powers of 4 from the payload of one TCP segment up to `net.ipv4.tcp_rmem`'s maximum, read on the machine running the flag, plus 256KB, the one upload size of the runs before the sweep, so new rows compare with theirs:

- The segment's payload is the smallest MTU among the `--interfaces`, read from `/sys/class/net/<device>/mtu` for the named device, the one holding the address, or for a wildcard address every device.
- From it come off the IPv4 header of RFC 791 or the IPv6 header of RFC 8200, and the TCP header of RFC 9293, 20 bytes without options, which leaves the MSS.
- The 12 bytes of RFC 7323's timestamps option come off too while `net.ipv4.tcp_timestamps` is on, as Linux then sends it on every segment.
- `net.ipv4.tcp_rmem`'s third field is the most one socket's receive buffer grows to, so the largest body fits one socket's buffer.

At MTU 9001 with timestamps and a 32MB `tcp_rmem` maximum that is 8,949 bytes through 9,163,776 bytes in six steps besides 256KB, and over loopback's 65,536-byte MTU, 65,484 bytes upward.
Agent fan-out: `POST /fanout?width=8` forwards the call to eight upstream `/tool` servers, checks each reply, and answers with 256 bytes.
Batched variants go through `/tool/batch` and `/fanout/batch`, only where the framework ships its own batcher.
A framework that would need a shim for a workload has no route for it, and its client writes an `unsupported` row for that configuration, naming the reason.
LitServe has no context upload or download route, so those rows read "server does not serve this call".

The gRPC pairs carry the same shapes through `serveval.proto`, padded to the same 1024 and 256 bytes: `Tool` for `body`, `Raw` for `raw`, and `Full` for `full`, with the token and `traceparent` as metadata entries.
Uploads stream `Chunk` messages into `Context`, and downloads ask `Download` for a byte count and receive a stream of `Chunk` messages.
`Full` answers a wrong token with `UNAUTHENTICATED` and a bad `traceparent` with `INVALID_ARGUMENT`.
gRPC pairs use HTTP/2, multiplexing calls over one channel per connection, and their fan-out holds one channel per upstream.

Bodies come from SplitMix64 under `--seed`, identical in Rust and Python.
`python3 payloads.py` and `cargo test --lib payloads` print and check the same four SHA-256 digests.

## Secure Mode

`--security tls` makes every competitor terminate TLS 1.3 itself, with AES-256-GCM, X25519 and no resumption.
Rust pairs set that policy through rustls, and Python servers through `openssl.cnf`, which they load as `OPENSSL_CONF`.
grpc.aio bundles BoringSSL, which ignores `openssl.cnf`, so its reports leave the suite to negotiation.
`keys.sh` writes a P-256 CA and a server leaf naming every `SERVEVAL_INTERFACES` address, plus Noise keys when UCall's `ucall` tool is on `PATH`.
TLS authenticates only the server, while Noise XX and IK authenticate both sides.
`--phases steady,handshake` reports reused connections and one connection per call apart.

## Placement

`launch.py` knows binary paths, interfaces and layouts, never framework names.
`--server-cores-per-network-interface N` gives a server N cores per interface: from the interface's memory domain, past its first `--housekeeping-cores-per-memory-domain` cores, inside the launcher's affinity mask, and without any core a network device's interrupt is pinned to, read from each device's MSI interrupts and `/proc/irq/<irq>/smp_affinity_list`.
With `--server`, one launch runs a whole pair from the client box: for each cores value and each connection count it starts the server on `--server-host` over ssh, waits until every target accepts, runs the client once per `--client-passes` entry, and stops the server by closing the ssh session's input, so every server is sized for one count and one budget.
A target that already accepts before its server starts fails that step, so no row measures a leftover server.
`SERVEVAL_SERVER_CPU_LISTS`, like `8-11/104-107` with one list per interface, pins the server's cores instead of deriving them, as when interrupts move onto the serving cores; a pair then takes one cores value, which only names the budget its rows record.
`--layout one` runs one process listing every interface, as UCall does.
`--layout per-domain` runs one process per interface on its NIC's memory domain, as competitors do.
Domains come from UCall's Python binding when it is importable, and are -1 otherwise.
The launcher's affinity mask and cgroup `memory.max` are split evenly across the processes it starts.
As root with systemd each process is a transient unit with `AllowedCPUs`, `MemoryMax` and a NUMA binding; otherwise it is a plain process pinned by affinity.
Kernel limits are printed and recorded in every report, never set: `nr_open`, `somaxconn`, `ip_local_port_range`, `tcp_max_syn_backlog`, `tcp_max_tw_buckets`, `tcp_tw_reuse`, `tcp_rmem`, `tcp_wmem` and `nf_conntrack_max`.
The 1M connection shape needs more than 64K source ports per target, so each NIC lists several addresses joined by `+`.
awc, httpx, LitServe, Ray Serve and both gRPC files bind by address alone, so they refuse an interface that names a device.
The grpc.aio client cannot bind a source address either, so its 1M reports are capped by routing.
Client processes meet at a barrier directory `launch.py` creates, so each configuration's window opens on all of them together, 100 ms after the last one finished connecting.

## Results

Every table below holds one row per machine and core class, so a new machine is a new row and nothing else moves.
Each framework is driven by its own client against its own server, and every handler only copies an `id` or counts bytes, so a row measures the framework rather than application code.
Servers and clients run inside the cores and memory the launcher's affinity mask and cgroup limit grant them, and the Machine column names the cores a row used.
Class is the capability class UCall reports for that machine, like `haswell` or `aws-g2`, for every framework's row.
Competitors run under `--layout per-domain`, one process per NIC on that NIC's memory domain, while UCall runs under `--layout one`, a single process whose event loops share every interface.
Steady rows reuse their connections, while handshake rows open one connection per call, so they price the TLS or Noise handshake.
Latency comes from open-loop runs under `--calls-per-second` and counts from each call's intended send time, so a queueing server cannot hide its backlog.
Throughput reads in calls per second, or Body GB/s for uploads, latency in µs, and server CPU in µs per call, counting the warmup.
Compare rows only within one table and at the same security, phase and connections.

### Tool Calls

`POST /tool` with 1024 bytes of JSON, answered with 256 bytes.

| Machine | Class | Framework | Client | Security | Phase | Connections | Throughput | p50 | p99 | Server CPU | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | ----------: | ---------: | --: | --: | ---------: | :--- |
| -       | -     | -         | -      | -        | -     |           - |          - |   - |   - |          - | -    |

### Agent Mesh

The same tool calls swept over 64, 10K and 1M connections, from one loopback box up to two boxes with two NICs each, with server memory per connection under `--between-runs reconnect`.

| Machine | Class | Framework | Client | Security | Phase | Connections | Throughput | p50 | p99 | Server CPU | Server Memory | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | ----------: | ---------: | --: | --: | ---------: | ------------: | :--- |
| -       | -     | -         | -      | -        | -     |           - |          - |   - |   - |          - |             - | -    |

### Context Uploads

`PUT /context` with `--body-bytes` seeded bytes, answered with the byte count.

| Machine | Class | Framework | Client | Security | Phase | Connections | Body | Throughput | p50 | p99 | Server CPU | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | ----------: | ---: | ---------: | --: | --: | ---------: | :--- |
| -       | -     | -         | -      | -        | -     |           - |    - |          - |   - |   - |          - | -    |

### Context Downloads

`GET /context?bytes=N`, answered with N seeded bytes, with the server's Gbit/s per card in each direction.

| Machine | Class | Framework | Client | Security | Phase | Connections | Body | Throughput | Gbit/s per card | p50 | p99 | Server CPU | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | ----------: | ---: | ---------: | --------------: | --: | --: | ---------: | :--- |
| -       | -     | -         | -      | -        | -     |           - |    - |          - |               - |   - |   - |          - | -    |

### Agent Fan-Out

`POST /fanout?width=8`, each call forwarded to eight upstream `/tool` servers and answered with 256 bytes.

| Machine | Class | Framework | Client | Security | Phase | Connections | Width | Throughput | p50 | p99 | Server CPU | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | ----------: | ----: | ---------: | --: | --: | ---------: | :--- |
| -       | -     | -         | -      | -        | -     |           - |     - |          - |   - |   - |          - | -    |

### Batched Variants

`/tool/batch` and `/fanout/batch`, only for frameworks that ship their own batcher.

| Machine | Class | Framework | Client | Security | Phase | Workload | Connections | Batch Size | Batch Wait | Throughput | p50 | p99 | Server CPU | Date |
| :------ | :---- | :-------- | :----- | :------- | :---- | :------- | ----------: | ---------: | ---------: | ---------: | --: | --: | ---------: | :--- |
| -       | -     | -         | -      | -        | -     | -        |           - |          - |          - |          - |   - |   - |          - | -    |

## CLI Reference

Every server and client, in every language, takes the same flags.
A value spelled `<…,...>` takes comma-separated values and runs one configuration per combination: clients sweep their own, and `launch.py` sweeps the server's.
A value spelled in the plural, like `<INTERFACES>`, is a comma-separated list that every configuration uses whole.
A combination a pair cannot run is still a row, with `unsupported` naming the reason and zeros where it would have measured, never a silent skip: batched uploads and downloads, a batched call to a server without a batcher, more calls in flight than a client multiplexes, or a route the server answers with 404, 405, 501 or gRPC's `UNIMPLEMENTED`.
Counts read `64`, `10K` or `1M` in powers of 1000, sizes read `256KB` or `4MB` in powers of 1024, and durations read `200ms` or `10s`.
Each client prints every setting at the start, as `- Name: value` in the same grammar the flag reads, then its cores and memory limit.
A bad value prints `--flag="value" does not parse, expected …` and exits with status 1.
Every flag also reads `SERVEVAL_<FLAG>` as its default, upper-cased with dashes as underscores, so `--time-limit` reads `SERVEVAL_TIME_LIMIT`.
The flag on the command line overrides the variable.

```text
Servers and clients
  --threads <N>                         0
      Workers, 0 for every core in the affinity mask: Tokio, actix and awc threads, httpx and grpc.aio client
      processes, Uvicorn and LitServe workers, Ray Serve replicas and gRPC server processes.
  --interfaces <INTERFACES>             127.0.0.1
      Each DEVICE=ADDRESS or ADDRESS, extra addresses joined by +.
  --port <PORT>                         8545
      Port every server listens on.
  --security <MODE>                     none
      none, noise-xx, noise-ik or tls.
  --connections <N,...>                 10K for servers, 64 for clients
      Swept by clients; the largest is the server's admission budget and listen backlog.
  --data-dir <DIR>                      data
      Directory of the keys keys.sh writes.
  --seed <N>                            42
      Seed of every payload, or random.

  --body-bytes <SIZE,...>               powers of 4 from one TCP segment's payload to tcp_rmem's maximum
      Context upload and download sizes; a server prepares the largest once for its downloads.

Servers
  --upstreams <UPSTREAMS>               none
      Fan-out targets, each ADDRESS:PORT.
  --batch-size <N>                      32
      Most calls one batch gathers, passed explicitly because the frameworks' defaults differ widely.
  --batch-wait <DURATION>               10ms
      Longest a partial batch's oldest call waits before it fires. The UCall servers fire it on the next poll
      after that, at most one poll slice later: the smaller of this wait and POLL_SLICE, 100 ms.

Clients
  --targets <ADDRESSES>                 127.0.0.1
      Server addresses, paired with --interfaces by position.
  --workloads <WORKLOAD,...>            tool,context,download,fanout
  --variants <VARIANT,...>              plain,batched
  --phases <PHASE,...>                  steady,handshake
  --parsing <SHAPE,...>                 body
      raw, body or full, swept only for plain tool calls.
  --calls-in-flight-per-connection <N|server-maximum,...>   1
      Calls each connection keeps outstanding, or the server's advertised stream or pipelining limit, RFC 9113's
      minimum of 100 when it advertises none.
  --calls-per-second <N>                none
      Open-loop arrival rate; unset runs a closed loop.
  --width <N>                           8
      Upstream calls per fan-out.
  --time-limit <DURATION>               10s
      Measured time per configuration.
  --warmup <DURATION>                   1s
      Unmeasured calls before each measured window.
  --memory-limit <SIZE>                 the cgroup's or the host's
      Client memory budget, refused above the cgroup's limit.
  --between-runs <POLICY>               reconnect
      keep reuses the open connections across configurations; reconnect closes them and opens fresh ones for each.
  --output <DIR>                        results
      Directory for JSON reports.
```

`launch.py` reads its own flags' `SERVEVAL_<FLAG>` defaults the same way, from `--port` on:

```text
  --layout <LAYOUT>                     one
      one, or per-domain for one process per interface on its NIC's memory domain.
  --copies <N>                          1
      Copies of a server on adjacent ports, for fan-out upstreams.
  --port <PORT>                         8545
      First port of the servers it starts, each copy on the next.
  --server-cores-per-network-interface <N,...>   none
      Cores each server process gets per interface: the first N of the interface's memory domain past the
      housekeeping cores, inside the launcher's affinity mask, pinned to no network device's interrupt. A server
      launch takes one value; a pair sweeps several, relaunching the server for each.
  --housekeeping-cores-per-memory-domain <N>     0
      First cores of each memory domain left to the kernel, sshd and recorders.
  --server <COMMAND>                    none
      Makes the launch a pair: per cores value and connection count, a fresh server, the client after -- against
      it once per pass, then the server stopped.
  --server-host <SSH DESTINATION>       this box
      Where the pair starts its server, through this launcher at the same path and Python.
  --server-interfaces <INTERFACES>      the client's
      The pair server's SERVEVAL_INTERFACES; a wildcard address stands for the devices holding the targets.
  --client <PROGRAM,...>                the program after --
      Clients to sweep against one listening server, each replacing the program before the flags after --.
  --client-passes <FLAGS;...>           one pass
      Extra client flags per pass, like "--variants plain --calls-in-flight-per-connection server-maximum".
```

## Output Format

Every client prints one JSON report per configuration to stdout, a Markdown row to stderr, and writes `results/<framework>-<config-hash>.json` atomically, flushed to the drive before it replaces an older one.
The hash is FNV-1a over the compact JSON of `config`, identical in Rust and Python.
The row below is the Rust UCall pair's plain tool calls over 256 connections from the AWS Graviton4 run of November 26, 2026, with only its histogram buckets cut short.

```json
{
  "schema_version": 3, "started_unix_seconds": 1795670850,
  "machine": { "cpu_model": "Neoverse-V2", "logical_cores": 192, "physical_cores": 192, "ram_bytes": 404875616256,
               "kernel": "7.0.0-1018-aws", "os": "Linux (Ubuntu 26.04)", "architecture": "aarch64",
               "kernel_limits": { "fs.nr_open": "4194304", "net.core.somaxconn": "65535",
                                  "net.ipv4.ip_local_port_range": "1024 65535", "net.ipv4.tcp_max_syn_backlog": "65535",
                                  "net.ipv4.tcp_max_tw_buckets": "2000000", "net.ipv4.tcp_rmem": "4096 131072 33554432",
                                  "net.ipv4.tcp_tw_reuse": "1", "net.ipv4.tcp_wmem": "4096 16384 4194304",
                                  "net.netfilter.nf_conntrack_max": "1048576" },
               "budget": { "cores": 128, "cpus": "0-63,96-159", "memory_limit": 395693158400 } },
  "config": { "framework": "ucall", "client": "ucall", "language": "rust", "test": "latency", "workload": "tool",
              "variant": "plain", "parsing": "body", "security": "none", "phase": "steady", "process": 0,
              "processes": 1, "interfaces": 2, "threads": 128, "connections": 256,
              "calls_in_flight_per_connection": 1, "calls_in_flight_per_connection_source": "flag", "in_flight": 256,
              "pipeline_depth": 1, "body_bytes": 0, "width": 8, "offered_calls_per_second": null,
              "server_cores_per_network_interface": 4, "seed": 42, "between_runs": "reconnect" },
  "unsupported": null, "tls_suite": null, "domains": null,
  "seconds": 10.0, "window_unix_ms": [1795670852464, 1795670862464],
  "calls": 9769884, "errors": 0,
  "error_classes": { "connect": 0, "refused": 0, "timeout": 0, "status": 0, "wrong_reply": 0 },
  "unsent": 0, "all_calls": 11726221,
  "request_body_bytes": 10004361216, "response_body_bytes": 2501090304, "reconnects": 260, "upstream_connections": 158,
  "server_units": ["serveval-server-ucall-8545-0.service"], "server_cpu_list": "8-11,104-107", "server_cores": 8,
  "server_memory_limit": null, "server_cpu_microseconds": 82888821,
  "server_memory_peak_bytes": 249774080, "server_memory_idle_peak_bytes": 102551552,
  "client_cpu_seconds": 139.200314, "calls_per_second": 976988.4, "calls_per_second_per_server_core": 122123.55,
  "body_gigabytes_per_second": 1.250545152,
  "latency_us": { "p50": 253.951, "p90": 376.831, "p99": 491.519, "p999": 589.823, "max": 950.271 },
  "latency_buckets": [[208, 2], [209, 3], "..."],
  "send_lateness_us": { "p50": 0.071, "p90": 0.071, "p99": 0.271, "p999": 0.287, "max": 32.767 },
  "send_lateness_buckets": [[46, 28], [47, 228], "..."],
  "server_cpu_microseconds_per_call": 7.068672933931571,
  "client_cpu_microseconds_per_call": 11.870858821439576,
  "server_busy_share": 0.8632514324806388, "server_kilobytes_per_connection": 561.609375,
  "server_event_loop_counters": { "accepts": 514, "bytes_copied": 16751078798, "bytes_received": 13749124555,
                                  "bytes_sent": 4737471845, "completions": 23455877, "copying_sends": 11726479,
                                  "empty_polls": 8, "kernel_enters": 443803, "polls": 445369,
                                  "ready_passes": 13879091, "receive_rearms": 0, "requests": 11726479,
                                  "submissions": 23455619, "zero_copy_sends": 0 },
  "server_event_loop_counters_per_request": { "accepts": 4.383242403794012e-05, "bytes_copied": 1428.4832470172846,
                                            "bytes_received": 1172.4853261580054, "bytes_sent": 403.9978108518337,
                                            "completions": 2.000248923824449, "copying_sends": 1.0,
                                            "empty_polls": 6.82216716543815e-07,
                                            "kernel_enters": 0.037846228181536844, "polls": 0.0379797721038003,
                                            "ready_passes": 1.1835684863291018, "requests": 1.0,
                                            "submissions": 2.0002269223353406 },
  "client_cpu_percent": 1160.0026166666667, "client_cpu_share": 0.09062520442708333, "client_limited": false
}
```

The layout is schema version 3, which `scripts/plot.py` checks; it still reads version 2 rows and their older names, like `server_cpu_us_per_call` and `server_kb_per_connection`.
`machine.budget` is the client's affinity mask and memory limit.
`server_cores` and `server_memory_limit` are the servers' affinity cores and smallest `memory.max`, summed over the targets, `server_cpu_list` the union of their cores, and `server_units` the systemd units measured, each once.
`unsupported` names why a configuration could not run, null when it ran.
`config.test` is `latency` for a plain row with one call in flight per connection, and `throughput` for a batched row or one with more.
`config.calls_in_flight_per_connection` is the depth the row ran, and `config.calls_in_flight_per_connection_source` says where it came from: `flag`, `server-advertised` for `server-maximum` reading the server's limit, or `rfc9113-minimum` for a server that advertises none.
`config.server_cores_per_network_interface` is the `--server-cores-per-network-interface` the servers ran under, as `launch.py` passes it to the client, null when none was given.
`machine.kernel_limits` holds the sysctls that cap connection counts and socket memory, `null` where `/proc/sys` lacks one.
`config.parsing` names the request shape, and is `body` for every workload but plain tool calls.
`config.client` names the client program that measured the row, so a client driving another stack's server still names itself, and `config.framework` names the server from its `/health`.
`config.batch_size` is the server's `--batch-size`, which `/health` reports as `batch_size`, null for a server that does not batch; it appears only in batched rows, so plain rows hash as they did before it was recorded.
`window_unix_ms` is the measured window's first and last Unix millisecond, to line the row up with `scripts/record.sh` samples.
`error_classes` splits `errors` into connect, refused, timeout, status and wrong reply, the last counting replies that differ from the expected bytes.
`unsent` counts calls scheduled inside the window that never went out, and `send_lateness_us` how late the sent ones left against their intended send time.
`client_cpu_share` is the client's CPU time as a share of its own cores, from 0 to 1.
`client_limited` is true when under 99% of the offered rate went out, when the p99 send lateness exceeds 5% of the p50 latency, or when the client used over 90% of its own cores, so the row measured the client rather than the server.
`server_busy_share` is `server_cpu_microseconds_per_call` times the window's calls per second, failed ones included, as a share of `server_cores`: 1 means the rate kept every serving core busy.
Well below 1, `calls_per_second_per_server_core` is not the server's capacity: a client whose threads contend, too few calls in flight, or a lock the server sleeps on held it back, and `client_limited` cannot tell those apart.
`request_body_bytes` and `response_body_bytes` count bodies only, without headers or framing, and `body_gigabytes_per_second` divides their sum by the window, printed as Body GB/s.
`reconnects` counts the TCP connections the client's box opened over the window, warmup included, and `upstream_connections` those the server's box opened from before connecting to the end, like fan-out upstreams.
Both read `ActiveOpens` from `/proc/net/snmp`, so they count TCP opens box-wide, any other process on the box included.
A steady row with nonzero `reconnects` lost connections it was meant to reuse.
Latency buckets are the sparse histogram, 16 linear bins per power of two nanoseconds, so percentiles merge exactly across processes and languages.
Open-loop latency counts from each call's intended send time, so a queueing server cannot hide its backlog.
Servers answer `GET /stats` from their own cgroup v2 `cpu.stat`, `memory.current` and `memory.peak`, which cover every worker process and the kernel's socket memory.
`server_cpu_microseconds_per_call` and `client_cpu_microseconds_per_call` divide each side's CPU time by every call including the warmup.
`server_memory_idle_peak_bytes` is the servers' `memory.peak` over the idle time before connecting, and `server_memory_peak_bytes` from before connecting to the window's end.
`server_kilobytes_per_connection` divides the growth of the peak over the idle peak by the connections, and holds only under `--between-runs reconnect`, since `keep` samples the server with the earlier connections already open.
`server_event_loop_counters` is the growth of the serving library's own event-loop counters over the warmup and window, like UCall's `kernel_enters`, null for libraries that keep none, and `server_event_loop_counters_per_request` divides each nonzero one by `requests`.
`launch.py` merges the reports of several client processes into one, counting every interface and memory domain it placed them on.
Compare only reports with the same machine, connections, security, phase and parsing.

`scripts/record.sh` runs on both boxes for the whole run and samples once per second: `ethtool -S` per ENA card, `nstat -az`, `/proc/interrupts` and `/proc/net/sockstat`, each stamped with Unix milliseconds, beside `mpstat -P ALL 1` and `sar -n DEV,EDEV 1`.
A row is void when `bw_in_allowance_exceeded`, `bw_out_allowance_exceeded`, `pps_allowance_exceeded`, `conntrack_allowance_exceeded` or `linklocal_allowance_exceeded` rises inside its window on either box, or when the samples do not cover its window.
Void rows are listed apart, naming the counter, and never plotted or published.

```sh
scripts/record.sh records/server            # on the server box, until interrupted
scripts/record.sh records/client            # on the client box, until interrupted
python3 scripts/plot.py results/ --records records/server --records records/client
uv run scripts/plot.py results/ --output plots/
```

The plotting script prints one Markdown table per workload, or with `--output` draws throughput and p99 latency against connections as SVG.
With `--records`, each row carries `server_gigabits_per_second_received_per_network_interface` and `server_gigabits_per_second_transmitted_per_network_interface`, mapping every server card to its rate over the row's window, beside the full `network_interfaces` figures, and the tables show them as Gbit/s in and out per card.
`--json-output` writes every row with those fields, like the `rows.json` of a run.
A row with any failed call stays out of the figures and their numbers, listed as `errors: N` beside unsupported, void and client-limited rows.

## Project Layout

The files sit flat at the root on purpose: every pair is one server and one client file beside the helpers both languages mirror.

- `Cargo.toml` names one `serv-eval-*` binary per Rust server and client, behind its framework's feature.
- `lib.rs` is the Rust library root the binaries share.
- `arguments.rs` and `arguments.py` hold the flags, the configuration sweep and socket placement.
- `payloads.rs` and `payloads.py` generate the seeded bodies and fixed-size replies.
- `results.rs` and `results.py` assemble, print and atomically write the reports.
- `stats.rs` and `stats.py` hold the latency histograms, the call schedule and the cgroup counters.
- `security.rs` and `security.py` set up TLS 1.3.
- `serveval.proto` defines the gRPC messages, and `build.rs` generates them for tonic and names the UCall tree the UCall pair links.
- `ucall_server.rs`, `axum_server.rs`, `actix_server.rs` and `tonic_server.rs` are the Rust servers.
- `ucall_client.rs`, `hyper_client.rs`, `awc_client.rs` and `tonic_client.rs` are the Rust clients.
- `ucall_server.py`, `fastapi_server.py`, `litserve_server.py`, `rayserve_server.py` and `grpc_server.py` are the Python servers.
- `ucall_client.py`, `httpx_client.py` and `grpc_client.py` are the Python clients.
- `launch.py` places servers and clients on cores, memory and NICs, and merges multi-process reports.
- `keys.sh` and `openssl.cnf` write the keys and pin the TLS policy.
- `scripts/record.sh` samples the NIC, socket and CPU counters that void throttled rows.
- `scripts/plot.py` prints and plots the reports, setting void rows apart.
- `scripts/article.py` draws the article's figures from the rows measured so far, with an index and the headline numbers.

## Benchmark Pitfalls

- AWS caps one TCP flow at 10 Gbps inside a cluster placement group and 5 Gbps outside it, so filling a 300 Gbps card takes at least 30 busy connections.
  A context-upload row over fewer connections measures the cap, not the framework.
- Security groups track every flow they do not fully allow, and when the tracking allowance fills, `conntrack_allowance_exceeded` rises and new connections drop long before ports run out.
  The benchmark interfaces sit in a security group allowing all traffic in and out, which AWS leaves untracked, while ssh uses a separate management interface.
- One source address reaches one target port through at most the 64,512 ports of `ip_local_port_range` 1024–65535, so 1M connections need at least 16 source addresses across the cards.
- The side that closes first holds each socket in TIME_WAIT for 60 s, so a reconnect or handshake sweep can starve the next configuration of ports.
  The client box sets `tcp_tw_reuse=1` and `tcp_max_tw_buckets` to 2M, `--between-runs reconnect` waits up to 10 s until the box's in-use and orphaned TCP sockets and the server's memory stop moving, and handshake rows run last, each after `ss -s` shows no TIME_WAIT.
- A saturated client reports its own ceiling as the server's, and an open-loop client that falls behind sends late calls that hide the backlog.
  Rows with `client_limited` set stay out of [Results](#results), and each client's ceiling is measured against the fastest server first.
- Uvicorn, LitServe and actix close idle connections after 5 s by default, which silently turns open-loop steady rows into reconnects.
  ServEval pins actix to the OS keep-alive, Uvicorn, LitServe and Ray Serve to a day, and awc's pooled connections to never expire, so a steady row's `reconnects` should read zero.
- Every client sends the same headers with `accept-encoding: identity`, so no server compresses a reply and no default header adds bytes to one framework's calls.
  awc drops its default headers, and httpx drops `accept`, `connection` and `user-agent`.
- The gRPC pairs run HTTP/2 and multiplex many calls over each channel, while the HTTP pairs run HTTP/1.1 with one call in flight per connection.
  Compare gRPC and HTTP rows at the same calls in flight, not only the same connections.
- Python clients reach their ceiling far below the Rust servers, so give httpx and grpc.aio more cores and sweep their processes until throughput plateaus.

## Remaining Scope

The following planned capabilities remain incomplete:

- UCall's client binds by address, not by device, so the AWS pair routes each card's traffic by putting every card on its own subnet.
- UCall's server runs one process whose event loops share every interface, while per-interface event loops on each NIC's memory domain wait for later.
- Reports carry `pipeline_depth`, which every client holds at 1, so no row pipelines HTTP/1.1 calls yet.

## Development

The repository pins Rust 1.100.
CI runs formatting, Ruff, `cargo clippy`, `cargo test --lib` and a loopback exchange per Rust feature.
The tonic pair needs `protoc` on `PATH`, and `grpc_server.py` loads `serveval.proto` through `grpcio-tools` at startup.
The UCall pairs build against the pinned UCall, which a local `.cargo/config.toml` may point at a sibling checkout.
The Python UCall pair keeps its free-threaded environment apart, as `.venv-ucall`, so it never shares pins with the other groups:
`UV_PROJECT_ENVIRONMENT=.venv-ucall uv sync --python 3.14t --group ucall` makes it, `uv pip install --python .venv-ucall ../UCall` swaps in a sibling checkout, and `.venv-ucall/bin/python` runs its server and client.
Arm the hooks once per clone with `git config core.hooksPath .githooks`.
The pre-commit hook checks the staged lines against the house style, with rustfmt, Ruff and markdownlint where they are installed.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --lib
python3 payloads.py
uvx ruff check && uvx ruff format --check
```

## History

ServEval inherits the cross-framework comparisons that lived in UCall's `bench/` folder.
There, a 1 KiB echo and a typed sum compared UCall with axum, actix-web, tonic, FastAPI and Python gRPC, driven by UCall's own load generator over HTTP and a tonic client over gRPC, with FastAPI and LitServe as batched inference baselines.
Those measurements stay in [CHANGELOG.md](CHANGELOG.md) as pre-ServEval rows.
They are not ServEval rows: one driver measured every HTTP framework, and their workloads predate tool calls, context uploads and fan-out.
