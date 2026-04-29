# Mini Key-Value Store

A small Memcached-leaning key-value store built for the "Building a Key-Value
Store From Scratch" series.

The current implementation provides a TCP text protocol backed by an in-memory
key-to-blob store with optional TTLs, deterministic memory limits, and LRU
eviction. There is no persistence, replication, or clustering yet.

## Requirements

- Go 1.22 or newer
- Docker, optional

## Run

By default the server listens on all interfaces so the same binary works inside
containers. For local-only development, pass a loopback address explicitly.

```sh
go run ./cmd/minikv
go run ./cmd/minikv -addr 127.0.0.1:11211
```

The server logs the bound address and accepts one command stream per TCP
connection.

Useful startup limits:

```sh
go run ./cmd/minikv \
  -addr 127.0.0.1:11211 \
  -max-value-bytes 1048576 \
  -max-memory-bytes 67108864 \
  -item-overhead-bytes 64 \
  -cleanup-interval 1m
```

The same commands are available through `make`:

```sh
make test
make run ADDR=127.0.0.1:11211
```

Memory accounting is intentionally explicit rather than pretending to match Go's
runtime/map overhead exactly: each item counts `len(key) + len(value) +
item-overhead-bytes`.

When a write would exceed `max-memory-bytes`, the store first removes expired
items, then evicts least-recently-used live items until the write fits. `get`,
successful `set`, and successful `incr` refresh recency. `delete` removes the
item and its accounted bytes. If a single item cannot fit under the memory
limit, the write fails with `SERVER_ERROR memory limit exceeded` and an existing
value for that key is left unchanged.

## Docker

Build the production image:

```sh
docker build -t mini-kv-store .
```

Run it with the default cache limits:

```sh
docker run --rm -p 11211:11211 mini-kv-store
```

Override the same operational flags you would use locally:

```sh
docker run --rm -p 11211:11211 mini-kv-store \
  -addr 0.0.0.0:11211 \
  -max-value-bytes 1048576 \
  -max-memory-bytes 67108864 \
  -item-overhead-bytes 64 \
  -cleanup-interval 1m
```

Smoke-test a running server over the TCP protocol:

```sh
printf 'ping\r\n' | nc 127.0.0.1 11211
```

Expected response:

```text
PONG
```

## Test

```sh
go test ./...
```

## Protocol

Commands and responses use CRLF line endings. Keys are non-empty, have no
whitespace/control characters, and are capped at 250 bytes.

### `ping`

Health/smoke command for operators and scripts.

```text
ping\r\n
```

Response:

```text
PONG\r\n
```

### `set`

Stores an exact byte blob. The value is byte-counted so spaces and binary-ish
payloads do not need escaping. The legacy form stores a non-expiring value:

```text
set <key> <bytes>\r\n
<value>\r\n
```

The TTL form expires the key after the given number of seconds. `0` means no
expiration.

```text
set <key> <ttl-seconds> <bytes>\r\n
<value>\r\n
```

Response:

```text
STORED\r\n
SERVER_ERROR value too large\r\n
SERVER_ERROR memory limit exceeded\r\n
```

Under memory pressure, `set` may evict older keys before returning `STORED`.

### `get`

```text
get <key>\r\n
```

Found response:

```text
VALUE <key> <bytes>\r\n
<value>\r\n
END\r\n
```

Missing response:

```text
END\r\n
```

Expired keys are treated as missing and are cleaned up lazily on access.

### `delete`

```text
delete <key>\r\n
```

Responses:

```text
DELETED\r\n
NOT_FOUND\r\n
```

Deleting an expired key returns `NOT_FOUND`.

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

Incrementing an expired key returns `NOT_FOUND`.
Successful increments refresh recency and may evict older keys if the rewritten
counter needs more accounted bytes.
