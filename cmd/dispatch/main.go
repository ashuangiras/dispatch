// Command dispatch is an MCP server (stdio) that runs commands asynchronously
// and retains their complete output for precise inspection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ashuangiras/dispatch/internal/runs"
	"github.com/ashuangiras/dispatch/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dispatch:", err)
		os.Exit(1)
	}
}

func defaultStateDir() string {
	if d := os.Getenv("DISPATCH_STATE_DIR"); d != "" {
		return d
	}
	if d := os.Getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "dispatch")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "state", "dispatch")
	}
	return ""
}

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

func run() error {
	var cfg runs.Config
	flag.StringVar(&cfg.StateDir, "state-dir", defaultStateDir(), "directory for run records and output (env DISPATCH_STATE_DIR)")
	flag.IntVar(&cfg.MaxActive, "max-active", runs.DefaultMaxActive, "maximum concurrently running commands")
	flag.Int64Var(&cfg.MaxOutputBytes, "max-output-bytes", runs.DefaultMaxOutputBytes, "maximum stored output per run, both streams together")
	flag.IntVar(&cfg.MaxRetainedRuns, "max-retained-runs", runs.DefaultMaxRetainedRuns, "finished runs kept before the oldest are removed")
	flag.Int64Var(&cfg.MaxRetainedBytes, "max-retained-bytes", runs.DefaultMaxRetainedBytes, "total stored bytes kept before the oldest finished runs are removed")
	flag.DurationVar(&cfg.Retention, "retention", runs.DefaultRetention, "finished runs older than this are removed")
	flag.DurationVar(&cfg.DefaultKillGrace, "kill-grace", runs.DefaultKillGrace, "default time between SIGTERM and SIGKILL")
	shutdownGrace := flag.Duration("shutdown-timeout", 15*time.Second, "how long shutdown waits for runs to be terminated and recorded")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("dispatch", version())
		return nil
	}
	if flag.NArg() > 0 {
		return errors.New("unexpected arguments; dispatch speaks MCP over stdin/stdout")
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	m, err := runs.New(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err = server.New(m, version()).Run(ctx, &mcp.StdioTransport{})
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
	m.Shutdown(sctx)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, mcp.ErrConnectionClosed) {
		return err
	}
	return nil
}
