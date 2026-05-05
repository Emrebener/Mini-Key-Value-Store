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
go run ./cmd/minikv
```

The binary reads `./minikv.conf` (a sample is shipped at the repo root); pass
`-config <path>` to point it at a different file. You should see the server log
the bound address. In another terminal, talk to
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
- **Tune the cache** by editing the keys in [Configuration](#configuration).
- **Compare it to Redis and Memcached** with the [benchmark stack](#benchmark-comparison).
- **Profile it, probe it, or check its health** by setting
  `pprof-addr = 127.0.0.1:6060` in `minikv.conf`. The same listener serves
  `/debug/pprof/` (for `go tool pprof`), `/healthz` (liveness), and `/doctor`
  (diagnostic checks). The Compose stack publishes the listener on host port
  `16060`.
- **Read the source.** The layout mirrors the design: `internal/protocol`
  parses commands; `internal/store` owns the sharded map (`store.go`,
  `shard.go`), the intrusive LRU (`lru.go`), and the eviction logic;
  `internal/server` wires them to TCP. Each layer has its own tests.

## Configuration

The binary reads a single key=value file. By default it looks for
`./minikv.conf` in the current directory; pass `-config <path>` to override.
Lines beginning with `#` are comments. Any key may be omitted to use its
default. The repository ships a sample `minikv.conf` at the root with every
key set to its default value:

```ini
addr                = 0.0.0.0:11211
pprof-addr          =
shards              = 16
max-value-bytes     = 1048576
max-memory-bytes    = 67108864
item-overhead-bytes = 64
cleanup-interval    = 1m
```

| Key | What it controls |
| --- | --- |
| `addr` | Listen address. Defaults to `0.0.0.0:11211` so the same binary works inside containers; use a loopback address for local-only use. |
| `shards` | Number of independently-locked shards over the keyspace. Each shard owns its own LRU and an equal slice of the memory budget. |
| `max-value-bytes` | Per-value byte cap. Larger values get rejected with `SERVER_ERROR value too large`. |
| `max-memory-bytes` | Total memory budget across all shards. When a write would exceed its shard's slice, the shard first removes expired items, then evicts least-recently-used live items until the write fits. |
| `item-overhead-bytes` | Per-item bookkeeping bytes added to `len(key) + len(value)`. Memory accounting is intentionally explicit rather than pretending to match Go's runtime/map overhead exactly. |
| `cleanup-interval` | How often the background sweeper removes expired keys. `0s` disables. Expired keys are also cleaned lazily on access. |
| `pprof-addr` | HTTP address for `net/http/pprof` handlers. Empty (the default) disables; set to e.g. `0.0.0.0:6060` to enable for benchmarking and debugging. |
| `tls-cert`, `tls-key` | PEM-encoded certificate and private key paths. Set both to wrap the listener in TLS; leave both empty for plain TCP. Setting only one is a configuration error. |
| `auth-token` | Bearer token required as the first command on each connection (`AUTH <token>\r\n`). Empty (the default) disables authentication. |

Unknown keys, malformed lines, and out-of-range numeric values are rejected at
startup with an error that names the file and line number.

A `Makefile` wraps the most common entry points:

```sh
make test
make run                          # ./cmd/minikv with the bundled minikv.conf
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

The image bakes the bundled `minikv.conf` in at `/minikv.conf`. Mount your own
to override:

```sh
docker run --rm -p 11211:11211 \
  -v $(pwd)/minikv.conf:/minikv.conf:ro \
  mini-kv-store
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

### `mget`

Multi-key fetch. Missing or expired keys are silently skipped; a single `END`
terminates the response.

```text
mget <key1> <key2> ...\r\n
```

Response:

```text
VALUE <key> <bytes>\r\n
<value>\r\n
... (one block per hit)
END\r\n
```

### `gets`

Like `get`, but the response includes the per-value CAS version. Accepts one or
more keys, same shape as `mget`.

```text
gets <key1> <key2> ...\r\n
```

Response:

```text
VALUE <key> <bytes> <cas>\r\n
<value>\r\n
...
END\r\n
```

### `cas`

Optimistic-concurrency write. Stores `value` only when the named key's current
CAS version matches the supplied one. The CAS version is the third token a
`gets` response carries.

```text
cas <key> <cas-version> <bytes>\r\n
<value>\r\n
```

TTL form:

```text
cas <key> <ttl-seconds> <cas-version> <bytes>\r\n
<value>\r\n
```

Responses:

```text
STORED\r\n          ← the version matched and the write went through
EXISTS\r\n          ← the key is present but its version differs
NOT_FOUND\r\n       ← the key is absent or expired
SERVER_ERROR value too large\r\n
SERVER_ERROR memory limit exceeded\r\n
```

The CAS version is a process-global, monotonically-increasing token stamped on
every successful `set`, `incr`, or `cas`. Tokens are unique across all keys for
the lifetime of the process, so a stale token from a key that was deleted and
recreated will never accidentally match the new value.

### `stats`

Operator-facing counters and store snapshot, in Memcached's `STAT` line shape.

```text
stats\r\n
```

Response:

```text
STAT uptime_seconds 142\r\n
STAT connections_opened 17\r\n
STAT connections_active 3\r\n
STAT cmd_set 2104\r\n
STAT cmd_get 9876\r\n
... (one STAT line per counter)
STAT items 1024\r\n
STAT memory_bytes 2097152\r\n
STAT max_memory_bytes 67108864\r\n
STAT evictions 0\r\n
STAT expirations 12\r\n
STAT shards 16\r\n
END\r\n
```

The HTTP listener (`pprof-addr`) also exposes `/healthz` (always returns 200
when the process is up) and `/doctor` (returns 200 with diagnostic lines when
all internal checks pass, 503 otherwise). `/doctor` currently checks memory
pressure and per-shard balance; both are intended to surface configuration
mismatches early rather than to be a full health board.

### `auth`

Bearer-token authentication. Required as the first command on each connection
when `auth-token` is set in the config.

```text
auth <token>\r\n
```

Responses: `AUTHENTICATED\r\n` (token matches), `CLIENT_ERROR auth failed\r\n`
(wrong token; connection closes), `CLIENT_ERROR auth required\r\n` (any
non-AUTH command before authentication; connection closes), or
`CLIENT_ERROR auth not configured\r\n` (server has no `auth-token`; connection
closes).

## Requirements

- Go 1.22 or newer (only if running outside Docker)
- Docker (optional, but used for the image and the benchmark stack)

## License

See repository for license details.
