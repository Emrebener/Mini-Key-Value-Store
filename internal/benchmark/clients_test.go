package benchmark

import (
	"bufio"
	"strings"
	"testing"
)

func TestReadMiniKVGetFound(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("VALUE alpha 5\r\nhello\r\nEND\r\n"))

	value, ok, err := readMiniKVGet(reader, "alpha")

	if err != nil {
		t.Fatalf("readMiniKVGet returned error: %v", err)
	}
	if !ok {
		t.Fatal("readMiniKVGet reported missing key")
	}
	if string(value) != "hello" {
		t.Fatalf("value = %q, want hello", value)
	}
}

func TestReadMiniKVGetMissing(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("END\r\n"))

	_, ok, err := readMiniKVGet(reader, "alpha")

	if err != nil {
		t.Fatalf("readMiniKVGet returned error: %v", err)
	}
	if ok {
		t.Fatal("readMiniKVGet reported found key")
	}
}

func TestReadRedisBulkString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))

	value, ok, err := readRedisBulkString(reader)

	if err != nil {
		t.Fatalf("readRedisBulkString returned error: %v", err)
	}
	if !ok {
		t.Fatal("readRedisBulkString reported nil")
	}
	if string(value) != "hello" {
		t.Fatalf("value = %q, want hello", value)
	}
}

func TestReadRedisNilBulkString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$-1\r\n"))

	_, ok, err := readRedisBulkString(reader)

	if err != nil {
		t.Fatalf("readRedisBulkString returned error: %v", err)
	}
	if ok {
		t.Fatal("readRedisBulkString reported found value")
	}
}

func TestReadMemcachedGetFound(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("VALUE alpha 0 5\r\nhello\r\nEND\r\n"))

	value, ok, err := readMemcachedGet(reader, "alpha")

	if err != nil {
		t.Fatalf("readMemcachedGet returned error: %v", err)
	}
	if !ok {
		t.Fatal("readMemcachedGet reported missing key")
	}
	if string(value) != "hello" {
		t.Fatalf("value = %q, want hello", value)
	}
}

func TestReadMemcachedGetMissing(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("END\r\n"))

	_, ok, err := readMemcachedGet(reader, "alpha")

	if err != nil {
		t.Fatalf("readMemcachedGet returned error: %v", err)
	}
	if ok {
		t.Fatal("readMemcachedGet reported found key")
	}
}
