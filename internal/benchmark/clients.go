package benchmark

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client interface {
	Name() string
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Close() error
}

func DialMiniKV(ctx context.Context, addr string, timeout time.Duration) (Client, error) {
	return dialTextClient(ctx, "minikv", addr, timeout, newMiniKVClient)
}

func DialRedis(ctx context.Context, addr string, timeout time.Duration) (Client, error) {
	return dialTextClient(ctx, "redis", addr, timeout, newRedisClient)
}

func DialMemcached(ctx context.Context, addr string, timeout time.Duration) (Client, error) {
	return dialTextClient(ctx, "memcached", addr, timeout, newMemcachedClient)
}

type textClient struct {
	name    string
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	timeout time.Duration
}

func dialTextClient(ctx context.Context, name string, addr string, timeout time.Duration, wrap func(textClient) Client) (Client, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return wrap(textClient{
		name:    name,
		conn:    conn,
		reader:  bufio.NewReader(conn),
		writer:  bufio.NewWriter(conn),
		timeout: timeout,
	}), nil
}

func (c *textClient) Name() string {
	return c.name
}

func (c *textClient) Close() error {
	return c.conn.Close()
}

func (c *textClient) prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.timeout == 0 {
		return nil
	}
	return c.conn.SetDeadline(time.Now().Add(c.timeout))
}

type miniKVClient struct {
	textClient
}

func newMiniKVClient(client textClient) Client {
	return &miniKVClient{textClient: client}
}

func (c *miniKVClient) Set(ctx context.Context, key string, value []byte) error {
	if err := c.prepare(ctx); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(c.writer, "set %s %d\r\n", key, len(value)); err != nil {
		return err
	}
	if _, err := c.writer.Write(value); err != nil {
		return err
	}
	if _, err := c.writer.WriteString("\r\n"); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	line, err := readCRLFLine(c.reader)
	if err != nil {
		return err
	}
	if line != "STORED" {
		return fmt.Errorf("minikv set %q returned %q", key, line)
	}
	return nil
}

func (c *miniKVClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := c.prepare(ctx); err != nil {
		return nil, false, err
	}
	if _, err := fmt.Fprintf(c.writer, "get %s\r\n", key); err != nil {
		return nil, false, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, false, err
	}
	return readMiniKVGet(c.reader, key)
}

func readMiniKVGet(reader *bufio.Reader, key string) ([]byte, bool, error) {
	line, err := readCRLFLine(reader)
	if err != nil {
		return nil, false, err
	}
	if line == "END" {
		return nil, false, nil
	}

	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != "VALUE" || fields[1] != key {
		return nil, false, fmt.Errorf("unexpected minikv get header %q", line)
	}
	size, err := strconv.Atoi(fields[2])
	if err != nil || size < 0 {
		return nil, false, fmt.Errorf("invalid minikv value size %q", fields[2])
	}
	value, err := readSizedValue(reader, size)
	if err != nil {
		return nil, false, err
	}
	end, err := readCRLFLine(reader)
	if err != nil {
		return nil, false, err
	}
	if end != "END" {
		return nil, false, fmt.Errorf("unexpected minikv get terminator %q", end)
	}
	return value, true, nil
}

type redisClient struct {
	textClient
}

func newRedisClient(client textClient) Client {
	return &redisClient{textClient: client}
}

func (c *redisClient) Set(ctx context.Context, key string, value []byte) error {
	if err := c.prepare(ctx); err != nil {
		return err
	}
	if err := writeRESPArray(c.writer, "SET", key, string(value)); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	line, err := readCRLFLine(c.reader)
	if err != nil {
		return err
	}
	if line != "+OK" {
		return fmt.Errorf("redis set %q returned %q", key, line)
	}
	return nil
}

func (c *redisClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := c.prepare(ctx); err != nil {
		return nil, false, err
	}
	if err := writeRESPArray(c.writer, "GET", key); err != nil {
		return nil, false, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, false, err
	}
	return readRedisBulkString(c.reader)
}

func writeRESPArray(writer *bufio.Writer, parts ...string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(parts)); err != nil {
		return err
	}
	for _, part := range parts {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(part), part); err != nil {
			return err
		}
	}
	return nil
}

func readRedisBulkString(reader *bufio.Reader) ([]byte, bool, error) {
	line, err := readCRLFLine(reader)
	if err != nil {
		return nil, false, err
	}
	if strings.HasPrefix(line, "-") {
		return nil, false, fmt.Errorf("redis error response: %s", line)
	}
	if line == "$-1" {
		return nil, false, nil
	}
	if !strings.HasPrefix(line, "$") {
		return nil, false, fmt.Errorf("unexpected redis bulk header %q", line)
	}
	size, err := strconv.Atoi(strings.TrimPrefix(line, "$"))
	if err != nil || size < 0 {
		return nil, false, fmt.Errorf("invalid redis bulk size %q", line)
	}
	value, err := readSizedValue(reader, size)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

type memcachedClient struct {
	textClient
}

func newMemcachedClient(client textClient) Client {
	return &memcachedClient{textClient: client}
}

func (c *memcachedClient) Set(ctx context.Context, key string, value []byte) error {
	if err := c.prepare(ctx); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(c.writer, "set %s 0 0 %d\r\n", key, len(value)); err != nil {
		return err
	}
	if _, err := c.writer.Write(value); err != nil {
		return err
	}
	if _, err := c.writer.WriteString("\r\n"); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	line, err := readCRLFLine(c.reader)
	if err != nil {
		return err
	}
	if line != "STORED" {
		return fmt.Errorf("memcached set %q returned %q", key, line)
	}
	return nil
}

func (c *memcachedClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := c.prepare(ctx); err != nil {
		return nil, false, err
	}
	if _, err := fmt.Fprintf(c.writer, "get %s\r\n", key); err != nil {
		return nil, false, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, false, err
	}
	return readMemcachedGet(c.reader, key)
}

func readMemcachedGet(reader *bufio.Reader, key string) ([]byte, bool, error) {
	line, err := readCRLFLine(reader)
	if err != nil {
		return nil, false, err
	}
	if line == "END" {
		return nil, false, nil
	}

	fields := strings.Fields(line)
	if len(fields) != 4 || fields[0] != "VALUE" || fields[1] != key {
		return nil, false, fmt.Errorf("unexpected memcached get header %q", line)
	}
	size, err := strconv.Atoi(fields[3])
	if err != nil || size < 0 {
		return nil, false, fmt.Errorf("invalid memcached value size %q", fields[3])
	}
	value, err := readSizedValue(reader, size)
	if err != nil {
		return nil, false, err
	}
	end, err := readCRLFLine(reader)
	if err != nil {
		return nil, false, err
	}
	if end != "END" {
		return nil, false, fmt.Errorf("unexpected memcached get terminator %q", end)
	}
	return value, true, nil
}

func readCRLFLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("line does not end with CRLF: %q", line)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func readSizedValue(reader *bufio.Reader, size int) ([]byte, error) {
	value := make([]byte, size+2)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, err
	}
	if value[size] != '\r' || value[size+1] != '\n' {
		return nil, fmt.Errorf("value does not end with CRLF")
	}
	return value[:size], nil
}
