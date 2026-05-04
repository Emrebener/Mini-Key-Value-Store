package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/protocol"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

func ServeConn(conn net.Conn, store *store.Store) error {
	defer conn.Close()
	return Serve(conn, conn, store)
}

func Serve(input io.Reader, output io.Writer, kv *store.Store) error {
	parser := protocol.NewParser(bufio.NewReader(input))
	writer := bufio.NewWriter(output)
	defer writer.Flush()

	for {
		command, err := parser.ReadCommand()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			writeLine(writer, "CLIENT_ERROR bad command")
			return writer.Flush()
		}
		if err := execute(writer, kv, command); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

func execute(writer *bufio.Writer, kv *store.Store, command protocol.Command) error {
	switch command.Op {
	case protocol.OpPing:
		return writeLine(writer, "PONG")
	case protocol.OpGet:
		item, ok := kv.Get(command.Key)
		if !ok {
			return writeLine(writer, "END")
		}
		if err := writeLine(writer, fmt.Sprintf("VALUE %s %d", command.Key, len(item.Value))); err != nil {
			return err
		}
		if _, err := writer.Write(item.Value); err != nil {
			return err
		}
		if _, err := writer.WriteString("\r\n"); err != nil {
			return err
		}
		return writeLine(writer, "END")
	case protocol.OpSet:
		if command.TTLSeconds > uint64(math.MaxInt64/int64(time.Second)) {
			return writeLine(writer, "CLIENT_ERROR TTL is too large")
		}
		ttl := time.Duration(command.TTLSeconds) * time.Second
		err := kv.Set(command.Key, command.Value, ttl)
		switch {
		case err == nil:
			return writeLine(writer, "STORED")
		case errors.Is(err, store.ErrValueTooLarge):
			return writeLine(writer, "SERVER_ERROR value too large")
		case errors.Is(err, store.ErrMemoryLimitExceeded):
			return writeLine(writer, "SERVER_ERROR memory limit exceeded")
		default:
			return err
		}
	case protocol.OpDelete:
		if kv.Delete(command.Key) {
			return writeLine(writer, "DELETED")
		}
		return writeLine(writer, "NOT_FOUND")
	case protocol.OpIncr:
		value, err := kv.Incr(command.Key, command.Delta)
		switch {
		case err == nil:
			return writeLine(writer, fmt.Sprintf("VALUE %d", value))
		case errors.Is(err, store.ErrNotFound):
			return writeLine(writer, "NOT_FOUND")
		case errors.Is(err, store.ErrNotInteger):
			return writeLine(writer, "CLIENT_ERROR value is not an unsigned integer")
		case errors.Is(err, store.ErrOverflow):
			return writeLine(writer, "CLIENT_ERROR increment would overflow uint64")
		case errors.Is(err, store.ErrValueTooLarge):
			return writeLine(writer, "SERVER_ERROR value too large")
		case errors.Is(err, store.ErrMemoryLimitExceeded):
			return writeLine(writer, "SERVER_ERROR memory limit exceeded")
		default:
			return err
		}
	default:
		return writeLine(writer, "CLIENT_ERROR unknown command")
	}
}

func writeLine(writer *bufio.Writer, line string) error {
	if _, err := writer.WriteString(line); err != nil {
		return err
	}
	_, err := writer.WriteString("\r\n")
	return err
}
