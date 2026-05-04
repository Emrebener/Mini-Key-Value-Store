<img width="2172" height="724" alt="image" src="https://github.com/user-attachments/assets/b73f32e9-badc-4ab7-9dfc-90c55d60ed98" />

# Mini Key-Value Store (MiniKV)

A small, Memcached-leaning key-value cache server written in Go from scratch.

MiniKV is the first project in the [Reinventing the Wheel](https://emrebener.com/topics/reinventing-the-wheel-series/)
series — building familiar tools from the ground up to understand the parts
tutorials usually skip: wire protocols that handle arbitrary bytes, consistent
expiration semantics, memory accounting that does not corrupt under load, and
eviction with defined behavior.

It is a learning project, not production software. There is no persistence,
replication, or clustering.

**What it does:** stores opaque byte values under string keys over TCP, with
optional per-key TTLs, a configurable memory budget, an intrusive-LRU eviction
policy once that budget is full, and a sharded keyspace for concurrent
throughput.

## Performance

The Compose stack benchmarks MiniKV against Redis and Memcached at 5 runs ×
1000 keys × 128-byte values, at concurrency=1 (sequential, single connection)
and concurrency=8 (eight parallel client connections). Numbers are medians of
three repeated runs.

| service   | workload | conc=1 | conc=8  |
| --------- | -------- | -----: | ------: |
| minikv    | write    | 47,921 | 215,635 |
| minikv    | read     | 46,716 | 221,014 |
| redis     | write    | 46,190 | 125,836 |
| redis     | read     | 46,393 | 135,852 |
| memcached | write    | 50,268 | 280,133 |
| memcached | read     | 48,819 | 281,252 |

Numbers are operations per second. Redis and Memcached columns are calibration
anchors, not a competition target — MiniKV is a learning project, not a
production peer. The journey to these numbers is covered in [the Reinventing
the Wheel series](https://emrebener.com/topics/reinventing-the-wheel-series/);
the [Benchmark comparison](#benchmark-comparison) section below covers how to
reproduce them.

## Getting started

The fastest way to try it — no Go toolchain needed — is Docker:

```sh
docker build -t mini-kv-store .
docker run --rm -p 11211:11211 mini-kv-store
```

Or, if you have Go 1.22+:

```sh
go run ./cmd/minikv -addr 127.0.0.1:11211
```

You should see the server log the bound address. In another terminal, talk to
it with `nc`:

```sh
printf 'ping\r\n' | nc 127.0.0.1 11211
# -> PONG

printf 'set greeting 5\r\nhello\r\n' | nc 127.0.0.1 11211
# -> STORED

printf 'get greeting\r\n' | nc 127.0.0.1 11211
# -> VALUE greeting 5
# -> hello
# -> END
```

That is the whole loop: connect, send a command line ending in `\r\n`, read the
response. The [Protocol](#protocol) section below lists every command.

## What you can do next

- **Read the blog posts.** Vol 1, [Building a Key-Value Store From
  Scratch](https://emrebener.com/topics/reinventing-the-wheel-series/building-a-key-value-store-from-scratch),
  walks through the original design and trade-offs. Vol 2 (linked from the
  [series page](https://emrebener.com/topics/reinventing-the-wheel-series/))
  covers the profiler-driven optimization journey behind the numbers in the
  Performance section above.
- **Run the tests:** `go test ./...` (or `make test`).
- **Tune the cache** with the flags in [Configuration](#configuration).
- **Compare it to Redis and Memcached** with the [benchmark stack](#benchmark-comparison).
- **Profile it** by passing `-pprof-addr 127.0.0.1:6060` to `cmd/minikv` and
  scraping `http://127.0.0.1:6060/debug/pprof/` with `go tool pprof`. The
  Compose stack publishes the same endpoint on host port `16060`.
- **Read the source.** The layout mirrors the design: `internal/protocol`
  parses commands; `internal/store` owns the sharded map (`store.go`,
  `shard.go`), the intrusive LRU (`lru.go`), and the eviction logic;
  `internal/server` wires them to TCP. Each layer has its own tests.

## Configuration

All knobs are command-line flags on `cmd/minikv`:

```sh
go run ./cmd/minikv \
  -addr 127.0.0.1:11211 \
  -shards 16 \
  -max-value-bytes 1048576 \
  -max-memory-bytes 67108864 \
  -item-overhead-bytes 64 \
  -cleanup-interval 1m \
  -pprof-addr ""
```

| Flag | What it controls |
| --- | --- |
| `-addr` | Listen address. Defaults to `0.0.0.0:11211` so the same binary works inside containers; pass a loopback address for local-only use. |
| `-shards` | Number of independently-locked shards over the keyspace. Each shard owns its own LRU and an equal slice of the memory budget. Defaults to 16. |
| `-max-value-bytes` | Per-value byte cap. Larger values get rejected with `SERVER_ERROR value too large`. |
| `-max-memory-bytes` | Total memory budget across all shards. When a write would exceed its shard's slice, the shard first removes expired items, then evicts least-recently-used live items until the write fits. |
| `-item-overhead-bytes` | Per-item bookkeeping bytes added to `len(key) + len(value)`. Memory accounting is intentionally explicit rather than pretending to match Go's runtime/map overhead exactly. |
| `-cleanup-interval` | How often the background sweeper removes expired keys. Expired keys are also cleaned lazily on access. |
| `-pprof-addr` | HTTP address for `net/http/pprof` handlers. Empty (the default) disables; set to e.g. `0.0.0.0:6060` to enable for benchmarking and debugging. |

The same commands are available through `make`:

```sh
make test
make run ADDR=127.0.0.1:11211
```

## Docker

Build the image:

```sh
docker build -t mini-kv-store .
```

Run it:

```sh
docker run --rm -p 11211:11211 mini-kv-store
```

Override flags the same way you would locally:

```sh
docker run --rm -p 11211:11211 mini-kv-store \
  -addr 0.0.0.0:11211 \
  -max-memory-bytes 67108864
```

## Benchmark comparison

The repository ships a local stack that benchmarks MiniKV against Redis and
Memcached. It is a calibration tool for this project, not a definitive database
ranking — results depend on the host, Docker engine, CPU scheduling, service
versions, and workload flags.

All three servers run in Docker, and the benchmark client runs in Docker on the
same Compose network. MiniKV uses its TCP text protocol; Redis and Memcached
use their native protocols (MiniKV does not implement either for compatibility).

```sh
make bench-stack-up   # docker compose up -d --build minikv redis memcached
make bench            # docker compose run --rm bench
make bench-stack-down # docker compose down
```

Host ports for inspection: MiniKV `11211` (with pprof on `16060`), Redis
`16379`, Memcached `11212`.

The benchmark writes `N` fixed-size values, reads them back, and repeats that
sequence many times for each service. Output is tab-separated:

```text
service	workload	count	min	mean	p50	p95	max	ops/sec
```

Useful flags:

```sh
docker compose run --rm bench \
  -runs 10 \
  -keys 5000 \
  -value-bytes 256 \
  -concurrency 8 \
  -services minikv,redis,memcached
```

`-concurrency N` dials N client connections per service and partitions each
run's keyspace across them, with a barrier between the write and read phases
per run. The default is 1, which reproduces the original single-connection
benchmark.

## Protocol

Commands and responses use CRLF (`\r\n`) line endings. Keys are non-empty, have
no whitespace or control characters, and are capped at 250 bytes.

### `ping`

Health/smoke command for operators and scripts.

```text
ping\r\n
```

Response: `PONG\r\n`.

### `set`

Stores an exact byte blob. The value is byte-counted, so spaces and binary
payloads do not need escaping.

Legacy form (no expiration):

```text
set <key> <bytes>\r\n
<value>\r\n
```

TTL form (`0` means no expiration):

```text
set <key> <ttl-seconds> <bytes>\r\n
<value>\r\n
```

Responses:

```text
STORED\r\n
SERVER_ERROR value too large\r\n
SERVER_ERROR memory limit exceeded\r\n
```

Under memory pressure, `set` may evict older keys before returning `STORED`. If
a single item cannot fit under the memory limit, the write fails and any
existing value for that key is left unchanged.

### `get`

```text
get <key>\r\n
```

Found:

```text
VALUE <key> <bytes>\r\n
<value>\r\n
END\r\n
```

Missing (or expired):

```text
END\r\n
```

Expired keys are treated as missing and cleaned up lazily on access.

### `delete`

```text
delete <key>\r\n
```

Responses: `DELETED\r\n` or `NOT_FOUND\r\n`. Deleting an expired key returns
`NOT_FOUND`.

### `incr`

Treats the stored blob as an unsigned base-10 integer, adds the delta, rewrites
the blob, and returns the new value.

```text
incr <key> <delta>\r\n
```

Responses:

```text
VALUE <new-value>\r\n
NOT_FOUND\r\n
CLIENT_ERROR value is not an unsigned integer\r\n
CLIENT_ERROR increment would overflow uint64\r\n
SERVER_ERROR value too large\r\n
SERVER_ERROR memory limit exceeded\r\n
```

Incrementing an expired key returns `NOT_FOUND`. Successful `get`, `set`, and
`incr` refresh recency for LRU; successful `incr` may evict older keys if the
rewritten counter needs more accounted bytes.

## Requirements

- Go 1.22 or newer (only if running outside Docker)
- Docker (optional, but used for the image and the benchmark stack)

## License

See repository for license details.
