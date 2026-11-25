# Changelog

Runs of ServEval and the measurements it builds on, one dated section per run day, newest first.
Every row names its machine and the date it was measured, and rows compare only within one table.

## November 2, 2026

### Before ServEval

UCall's own benchmark suite measured these rows on October 25 and November 2, 2026, before ServEval had its own server and client pairs.
They are not ServEval rows: the workloads were a 1 KiB echo and a typed sum rather than tool calls, context uploads and fan-out, and one driver measured every HTTP framework.
Every server answered from a trivial handler over loopback networking, on Release builds and without encryption.
UCall's `ucall_bench_http` drove every HTTP server closed-loop and verified each reply, after a 1 s warmup over an 8 s window.
It ran 2 threads over 64 connections with 256 calls in flight against one server thread, and 8 threads over 256 connections with 1,024 calls in flight against four.
The gRPC rows came from UCall's `grpc_client.rs` on tonic instead, closed-loop at 256 concurrent calls over 64 connections per server thread, over HTTP/2 with protobuf framing.
FastAPI ran on uvloop and httptools without access logs, with its threads counting Uvicorn worker processes, and the Python gRPC server's threads were processes sharing the port through `SO_REUSEPORT`.
The baselines were FastAPI 0.162 on Uvicorn 0.54 with uvloop 0.23 and httptools 0.9, grpcio 1.84 or 1.85, axum 0.8.9, actix-web 4.18.1 and tonic 0.14 on Tokio 1.54.

The Intel Core i9-13950HX rows ran Linux 7.3, GCC 16.2, Rust 1.99 and Python 3.14 on a shared desktop under the powersave governor, so they are indicative.
Every point waited for a one-minute load average below 4.
The server took the P-core CPUs 0 and 2, or the even CPUs 0 through 8 for four threads, and the driver CPUs 8 and 10, or the E-cores 16 through 31 for four threads.
The AWS Graviton4 rows ran on 96 Neoverse V2 cores in one memory domain, under Linux 7.0, Clang 23.1, Rust 1.99 and Python 3.13 on an otherwise idle machine.
The server took CPUs 16 and 17 and the driver CPUs 48 and 49, both forced to UCall's `apple-m2` CPU class, which has since been retired into `aws-g2`.

### Framework Echo

`POST /echo` with a 1 KiB body, a zero byte followed by `x` bytes, returned unchanged; the gRPC pairs carried the same bytes in a `Payload` message.
One server thread, with every exchange verified and none failed:

| Machine                                       | Framework    | Requests/s | p50, µs | p99, µs | Date       |
| :-------------------------------------------- | :----------- | ---------: | ------: | ------: | :--------- |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall native |    291,060 |     852 |   3,408 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Rust   |    278,201 |     852 |   3,801 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Python |    267,955 |     885 |   3,801 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | axum         |    226,963 |   1,180 |   1,245 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | actix-web    |    247,217 |   1,049 |   2,884 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | FastAPI      |     20,816 |  12,583 |  13,107 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Python  |     14,785 |  17,150 |  24,801 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Rust    |     82,657 |   3,593 |   5,116 | 2026-10-25 |
| AWS Graviton4, 96 cores, 1 domain             | UCall native |    198,540 |   1,311 |   1,442 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | UCall Rust   |    190,888 |   1,376 |   1,442 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | UCall Python |    164,662 |   1,573 |   1,769 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | axum         |    150,728 |   1,704 |   1,769 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | actix-web    |    165,570 |   1,573 |   1,573 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | FastAPI      |     16,759 |  15,729 |  15,729 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | gRPC Python  |      9,938 |  25,893 |  26,652 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | gRPC Rust    |     56,360 |   5,680 |   6,017 | 2026-11-02 |

Four server threads, for the frameworks that scale across threads:

| Machine                                       | Framework    | Requests/s | p50, µs | p99, µs | Date       |
| :-------------------------------------------- | :----------- | ---------: | ------: | ------: | :--------- |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall native |    873,040 |   1,180 |   1,442 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Rust   |    792,292 |   1,311 |   1,638 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | axum         |    621,157 |   1,638 |   2,228 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | actix-web    |    825,820 |   1,245 |   1,638 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Rust    |    187,511 |   5,092 |  13,469 | 2026-10-25 |

- UCall's Python bindings stayed within 8% of the native server on the Intel machine and 17% on Graviton4, because the handler runs on the polling thread with no event loop or coroutine in between.
- FastAPI and Python gRPC served about 13 and 18 times fewer echoes than those bindings on the same Intel core, and tonic about 3.2 times fewer.

### Framework Sum

`GET /sum?a=17&b=25` without a body, answered with the 2-byte `42`; the gRPC pairs answered a `Sum` call with a `Total` message.
One server thread:

| Machine                                       | Framework    | Requests/s | p50, µs | p99, µs | Date       |
| :-------------------------------------------- | :----------- | ---------: | ------: | ------: | :--------- |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall native |    439,711 |     590 |     721 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Rust   |    403,497 |     655 |     852 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Python |    366,810 |     688 |   3,146 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | axum         |    265,476 |     950 |   1,966 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | actix-web    |    280,936 |     885 |   1,049 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | FastAPI      |     13,880 |  18,874 |  20,972 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Python  |     15,040 |  16,833 |  22,078 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Rust    |    107,940 |   2,714 |   3,788 | 2026-10-25 |
| AWS Graviton4, 96 cores, 1 domain             | UCall native |    246,738 |   1,049 |   1,180 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | UCall Rust   |    232,030 |   1,114 |   1,180 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | UCall Python |    197,126 |   1,311 |   1,507 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | axum         |    167,745 |   1,573 |   1,573 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | actix-web    |    182,152 |   1,442 |   1,442 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | FastAPI      |     14,831 |  17,826 |  17,826 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | gRPC Python  |     10,142 |  25,340 |  26,053 | 2026-11-02 |
| AWS Graviton4, 96 cores, 1 domain             | gRPC Rust    |     73,448 |   4,345 |   4,640 | 2026-11-02 |

Four server threads:

| Machine                                       | Framework    | Requests/s | p50, µs | p99, µs | Date       |
| :-------------------------------------------- | :----------- | ---------: | ------: | ------: | :--------- |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall native |  1,015,842 |   1,016 |   1,311 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | UCall Rust   |  1,198,058 |     852 |     983 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | axum         |    726,956 |   1,442 |   1,835 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | actix-web    |    936,873 |   1,114 |   1,180 | 2026-10-25 |
| Intel Core i9-13950HX, 8P+16E cores, 1 domain | gRPC Rust    |    217,100 |   4,396 |  11,752 | 2026-10-25 |

- UCall's Python bindings parse and convert both parameters in C before calling the handler, and served 83% of the native rate and about 26 times FastAPI's on the Intel core.
