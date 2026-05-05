package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "minikv.conf")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoad_omittedKeysFallBackToDefaults(t *testing.T) {
	path := writeTempConfig(t, "addr = 127.0.0.1:9999\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Addr != "127.0.0.1:9999" {
		t.Errorf("Addr = %q, want %q", cfg.Addr, "127.0.0.1:9999")
	}
	if cfg.Shards != Default().Shards {
		t.Errorf("Shards = %d, want default %d", cfg.Shards, Default().Shards)
	}
	if cfg.CleanupInterval != Default().CleanupInterval {
		t.Errorf("CleanupInterval = %v, want default %v", cfg.CleanupInterval, Default().CleanupInterval)
	}
}

func TestLoad_commentsAndBlankLinesIgnored(t *testing.T) {
	path := writeTempConfig(t, `# minikv config

# the address
addr = 127.0.0.1:11211

shards = 32
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Addr != "127.0.0.1:11211" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.Shards != 32 {
		t.Errorf("Shards = %d, want 32", cfg.Shards)
	}
}

func TestLoad_whitespaceAroundEqualsAndKeysIsTolerated(t *testing.T) {
	path := writeTempConfig(t, "   addr   =    127.0.0.1:11211   \n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Addr != "127.0.0.1:11211" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
}

func TestLoad_emptyStringValueIsExplicitlyEmpty(t *testing.T) {
	path := writeTempConfig(t, "addr = 0.0.0.0:11211\npprof-addr =\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PprofAddr != "" {
		t.Errorf("PprofAddr = %q, want empty", cfg.PprofAddr)
	}
}

func TestLoad_emptyValueOnNumericFieldFails(t *testing.T) {
	path := writeTempConfig(t, "shards =\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "shards") {
		t.Errorf("error %q does not mention key", err)
	}
}

func TestLoad_unknownKeyRejectedWithLineNumber(t *testing.T) {
	path := writeTempConfig(t, "addr = 127.0.0.1:11211\nmax-memmory-bytes = 1\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error on unknown key, got nil")
	}
	if !strings.Contains(err.Error(), ":2:") {
		t.Errorf("error %q does not include line number 2", err)
	}
	if !strings.Contains(err.Error(), "max-memmory-bytes") {
		t.Errorf("error %q does not name the offending key", err)
	}
}

func TestLoad_malformedLineRejectedWithLineNumber(t *testing.T) {
	path := writeTempConfig(t, "addr = 127.0.0.1:11211\nthis line has no equals\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error on malformed line, got nil")
	}
	if !strings.Contains(err.Error(), ":2:") {
		t.Errorf("error %q does not include line number 2", err)
	}
}

func TestLoad_durationParsesAcceptedFormats(t *testing.T) {
	cases := map[string]time.Duration{
		"1m":  time.Minute,
		"30s": 30 * time.Second,
		"2h":  2 * time.Hour,
		"0s":  0,
	}
	for input, want := range cases {
		path := writeTempConfig(t, "cleanup-interval = "+input+"\n")
		cfg, err := Load(path)
		if err != nil {
			t.Errorf("Load(%q): %v", input, err)
			continue
		}
		if cfg.CleanupInterval != want {
			t.Errorf("CleanupInterval(%q) = %v, want %v", input, cfg.CleanupInterval, want)
		}
	}
}

func TestLoad_validateMaxValueLargerThanMaxMemoryRejected(t *testing.T) {
	path := writeTempConfig(t, "max-value-bytes = 100\nmax-memory-bytes = 50\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "max-value-bytes") {
		t.Errorf("error %q does not mention the offending field", err)
	}
}

func TestLoad_missingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.conf"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestDefault_validates(t *testing.T) {
	if err := Default().validate(); err != nil {
		t.Fatalf("Default() should validate cleanly, got %v", err)
	}
}

func TestLoad_tlsKeysMustBeSetTogether(t *testing.T) {
	cases := []string{
		"tls-cert = /etc/minikv/server.crt\n",
		"tls-key = /etc/minikv/server.key\n",
	}
	for _, contents := range cases {
		path := writeTempConfig(t, contents)
		_, err := Load(path)
		if err == nil {
			t.Errorf("expected validation error for %q, got nil", contents)
			continue
		}
		if !strings.Contains(err.Error(), "tls-cert") || !strings.Contains(err.Error(), "tls-key") {
			t.Errorf("error %q should name both tls keys", err)
		}
	}
}

func TestLoad_tlsKeysSetTogetherEnablesTLS(t *testing.T) {
	path := writeTempConfig(t, "tls-cert = /etc/minikv/server.crt\ntls-key = /etc/minikv/server.key\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.TLSEnabled() {
		t.Error("TLSEnabled() = false, want true")
	}
}

func TestLoad_authTokenAccepted(t *testing.T) {
	path := writeTempConfig(t, "auth-token = s3cret-bearer-value\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AuthToken != "s3cret-bearer-value" {
		t.Errorf("AuthToken = %q", cfg.AuthToken)
	}
}
