package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"
)

// =============================================================================
// Logging
// =============================================================================
//
// One logger, one format, and a level you can turn down. Before this the agent
// had 340 bare log.Printf calls: no levels, so a noisy node could not be
// quietened without patching the binary, and no way to tell a routine gossip
// message apart from a failed deploy.
//
// Configuration:
//
//	JETTY_LOG_LEVEL   debug | info (default) | warn | error
//	JETTY_LOG_FORMAT  text (default, logfmt) | json
//
// Deliberately still printf-style. Converting every message to key/value
// attributes is a per-site judgement call across 340 sites, and doing it in
// the same change as the level plumbing would make both unreviewable. Levels,
// a single format, and one configurable logger are the parts that pay off
// immediately; attributes can be promoted message by message afterwards.

var levelVar = new(slog.LevelVar)

// InitLogging configures process-wide logging from the environment. Call it
// first thing in main, before anything else has a chance to log.
func InitLogging() {
	level := initLogging(getEnv("JETTY_LOG_LEVEL", "info"), getEnv("JETTY_LOG_FORMAT", "text"), os.Stderr)
	if level != slog.LevelInfo {
		slog.Info("log level configured", "level", level.String())
	}
}

// initLogging configures the process-wide logger. Safe to call more than once;
// the last call wins. Returns the resolved level so startup can report it.
func initLogging(levelName, format string, out io.Writer) slog.Level {
	level := parseLogLevel(levelName)
	levelVar.Set(level)

	opts := &slog.HandlerOptions{
		Level: levelVar,
		// Source position is genuinely useful when chasing a bug and pure
		// noise otherwise, which maps exactly onto debug.
		AddSource: level <= slog.LevelDebug,
	}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}

	// SetDefault does double duty: it sets the slog default *and* repoints
	// the standard log package at this handler. That second part is what
	// captures memberlist, which writes to log.Default() and would otherwise
	// bypass the level and format entirely.
	//
	// Do not add a manual log.SetOutput bridge on top of this. Routing
	// log -> slog by hand while slog's own default routes slog -> log closes
	// a cycle, and the first line logged after anything resets the default
	// recurses until the stack dies.
	slog.SetDefault(slog.New(handler))

	return level
}

// parseLogLevel maps a name to a level, defaulting to info. An unrecognised
// value falls back rather than failing startup - a typo in an env var should
// not stop a node from booting.
func parseLogLevel(name string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Printf-style wrappers. These exist so the migration off log.Printf could be
// mechanical; prefer slog.Info with attributes for new code.

// logAt records at level with the *caller's* source position rather than this
// file's. Calling slog.Info from a wrapper attributes every line to log.go,
// which makes AddSource worse than useless - it points confidently at the
// wrong place. Capturing the PC ourselves is the only way to keep it honest.
func logAt(level slog.Level, format string, args ...any) {
	logger := slog.Default()
	ctx := context.Background()
	if !logger.Enabled(ctx, level) {
		return
	}
	// skip: 0=runtime.Callers, 1=logAt, 2=the logXf wrapper, 3=the caller.
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	rec := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, args...), pcs[0])
	_ = logger.Handler().Handle(ctx, rec)
}

func logDebugf(format string, args ...any) { logAt(slog.LevelDebug, format, args...) }
func logInfof(format string, args ...any)  { logAt(slog.LevelInfo, format, args...) }
func logWarnf(format string, args ...any)  { logAt(slog.LevelWarn, format, args...) }
func logErrorf(format string, args ...any) { logAt(slog.LevelError, format, args...) }

// logFatalf reports an unrecoverable startup failure and exits. Separate from
// logErrorf so the exit is visible at the call site.
func logFatalf(format string, args ...any) {
	logAt(slog.LevelError, format, args...)
	os.Exit(1)
}
