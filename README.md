# Mini Key-Value Store

A small Memcached-leaning key-value store built for the "Building a Key-Value
Store From Scratch" series.

Milestone 1 implements a TCP text protocol backed by an in-memory key-to-blob
store. There is no persistence, TTL, eviction, replication, or clustering yet.

## Requirements

- Go 1.22 or newer

## Run

```sh
go run ./cmd/minikv -addr 127.0.0.1:11211
```

The server logs the bound address and accepts one command stream per TCP
connection.

## Test

```sh
go test ./...
```

## Protocol

Commands and responses use CRLF line endings. Keys are non-empty, have no
whitespace/control characters, and are capped at 250 bytes.

### `set`

Stores an exact byte blob. The value is byte-counted so spaces and binary-ish
payloads do not need escaping.

```text
set <key> <bytes>\r\n
<value>\r\n
```

Response:

```text
STORED\r\n
```

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

### `delete`

```text
delete <key>\r\n
```

Responses:

```text
DELETED\r\n
NOT_FOUND\r\n
```

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
```
