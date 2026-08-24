package agent

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// restoreLogging puts global logging state back after a test mutates it.
// initLogging touches three globals - the slog default, the level, and the
// standard log package's output - and all three have to be restored together.
// Restoring only slog while leaving log pointed at a stale bridge is exactly
// the configuration that used to hang the suite.
func restoreLogging(t *testing.T) {
	t.Helper()
	prevLogger := slog.Default()
	prevLevel := levelVar.Level()
	prevOut := log.Writer()
	prevFlags := log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
		levelVar.Set(prevLevel)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
}

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"  warn ": slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"info":    slog.LevelInfo,
		"":        slog.LevelInfo,
		// A typo must not stop a node booting - fall back, don't fail.
		"verbose": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLogLevel(in); got != want {
			t.Errorf("parseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLogLevelFiltersOutput(t *testing.T) {
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("warn", "text", &buf)

	logDebugf("debug message")
	logInfof("info message")
	logWarnf("warn message")
	logErrorf("error message")

	out := buf.String()
	for _, suppressed := range []string{"debug message", "info message"} {
		if strings.Contains(out, suppressed) {
			t.Errorf("level=warn still emitted %q", suppressed)
		}
	}
	for _, kept := range []string{"warn message", "error message"} {
		if !strings.Contains(out, kept) {
			t.Errorf("level=warn dropped %q", kept)
		}
	}
}

func TestLogFormatJSON(t *testing.T) {
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("info", "json", &buf)
	logInfof("hello %s", "world")

	var entry map[string]interface{}
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("json format did not produce JSON: %v (line: %q)", err, line)
	}
	if entry["msg"] != "hello world" {
		t.Errorf("msg = %v, want %q", entry["msg"], "hello world")
	}
	if entry["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", entry["level"])
	}
}

func TestLoggingDoesNotRecurseAfterDefaultReset(t *testing.T) {
	// slog.SetDefault repoints the standard log package at the slog handler,
	// and slog's *built-in* default handler writes back out through log.
	// Adding a manual log.SetOutput bridge on top closes that cycle: the
	// first line logged after anything resets the default recurses until the
	// stack dies. That combination hung this whole suite for the full 600s
	// test timeout. We rely on SetDefault alone; this pins that.
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("info", "text", &buf)

	// Reset the default the way a test cleanup elsewhere would.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf("must not recurse")
		logErrorf("nor must this")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("logging recursed after the slog default was reset")
	}
}

func TestStdlogIsRoutedThroughSlog(t *testing.T) {
	// memberlist writes to the standard logger. Without the bridge those
	// lines bypass the level and the format entirely.
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("info", "json", &buf)
	log.Printf("from a dependency")

	var entry map[string]interface{}
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("stdlib log output was not routed through slog: %q", line)
	}
	if entry["msg"] != "from a dependency" {
		t.Errorf("msg = %v, want %q", entry["msg"], "from a dependency")
	}
}

func TestDebugSourceNamesTheCallerNotTheWrapper(t *testing.T) {
	// Calling slog.Info from inside logInfof attributes every line to log.go,
	// which points confidently at the wrong file. AddSource is only worth
	// having if it names the real call site.
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("debug", "json", &buf)
	logInfof("from the test")

	var entry struct {
		Source struct {
			File string `json:"file"`
		} `json:"source"`
	}
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("parse: %v (line %q)", err, line)
	}
	if got := filepath.Base(entry.Source.File); got != "log_test.go" {
		t.Errorf("source file = %q, want log_test.go — the wrapper is being blamed instead of the caller", got)
	}
}

func TestUnknownFormatFallsBackToText(t *testing.T) {
	restoreLogging(t)

	var buf bytes.Buffer
	initLogging("info", "yaml-please", &buf)
	logInfof("plain")

	line := strings.TrimSpace(buf.String())
	if strings.HasPrefix(line, "{") {
		t.Errorf("unknown format produced JSON, want text fallback: %q", line)
	}
	if !strings.Contains(line, "plain") {
		t.Errorf("message missing from output: %q", line)
	}
}

// TestNoBareLogPrintf mirrors TestNoBareHTTPErrorCalls: the migration is worth
// doing once, and without a guard the next handler written from muscle memory
// reintroduces an unleveled log line that no operator can filter.
//
// log.go is exempt - it owns the bridge that routes the standard logger into
// slog, and necessarily references the log package.
func TestNoBareLogPrintf(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "log.go" {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			for _, bad := range []string{"log.Printf(", "log.Println(", "log.Fatal"} {
				if strings.Contains(line, bad) {
					offenders = append(offenders, name+":"+itoa(i+1)+": "+trimmed)
				}
			}
		}
	}

	if len(offenders) > 0 {
		t.Errorf("the standard log package bypasses levels and format.\n"+
			"Use logInfof/logWarnf/logErrorf/logDebugf (or logFatalf). Offenders:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}
