// Package config loads MiniKV's runtime configuration from a key=value file.
//
// The file format is one key per line, key and value separated by '='. Lines
// starting with '#' and blank lines are ignored. Whitespace around the key
// and value is trimmed. Unknown keys are rejected with an error that names
// the file, line number, and offending key.
//
// The parser intentionally has no escaping rules and no nested structure.
// MiniKV's config has seven primitive fields; the file format mirrors that.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is MiniKV's runtime configuration. Exported fields mirror the keys
// accepted in the config file, kebab-cased.
type Config struct {
	Addr              string        // addr
	PprofAddr         string        // pprof-addr
	Shards            int           // shards
	MaxValueBytes     int           // max-value-bytes
	MaxMemoryBytes    int           // max-memory-bytes
	ItemOverheadBytes int           // item-overhead-bytes
	CleanupInterval   time.Duration // cleanup-interval
}

// Default returns the configuration MiniKV uses when no value is supplied
// for a key. The file format treats omitted keys as "use the default."
func Default() Config {
	return Config{
		Addr:              "0.0.0.0:11211",
		PprofAddr:         "",
		Shards:            16,
		MaxValueBytes:     1024 * 1024,
		MaxMemoryBytes:    64 * 1024 * 1024,
		ItemOverheadBytes: 64,
		CleanupInterval:   time.Minute,
	}
}

// Load reads path, parses every recognized key, validates the result, and
// returns the merged configuration. Defaults fill in any key not present.
// Errors include the file path and 1-based line number on parse failures.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	cfg := Default()
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("%s:%d: expected key = value", path, lineNo)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if err := setField(&cfg, key, value); err != nil {
			return Config{}, fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func setField(cfg *Config, key, value string) error {
	switch key {
	case "addr":
		cfg.Addr = value
	case "pprof-addr":
		cfg.PprofAddr = value
	case "shards":
		return parseInt(value, "shards", &cfg.Shards)
	case "max-value-bytes":
		return parseInt(value, "max-value-bytes", &cfg.MaxValueBytes)
	case "max-memory-bytes":
		return parseInt(value, "max-memory-bytes", &cfg.MaxMemoryBytes)
	case "item-overhead-bytes":
		return parseInt(value, "item-overhead-bytes", &cfg.ItemOverheadBytes)
	case "cleanup-interval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("cleanup-interval: %w", err)
		}
		cfg.CleanupInterval = d
	default:
		return fmt.Errorf("unknown key %q", key)
	}
	return nil
}

func parseInt(value, name string, dst *int) error {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*dst = n
	return nil
}

func (c Config) validate() error {
	if c.MaxValueBytes <= 0 {
		return errors.New("max-value-bytes must be positive")
	}
	if c.MaxMemoryBytes <= 0 {
		return errors.New("max-memory-bytes must be positive")
	}
	if c.ItemOverheadBytes < 0 {
		return errors.New("item-overhead-bytes must be non-negative")
	}
	if c.MaxValueBytes > c.MaxMemoryBytes {
		return errors.New("max-value-bytes must be less than or equal to max-memory-bytes")
	}
	if c.CleanupInterval < 0 {
		return errors.New("cleanup-interval must be non-negative")
	}
	if c.Shards <= 0 {
		return errors.New("shards must be positive")
	}
	return nil
}
