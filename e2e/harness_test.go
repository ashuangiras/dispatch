package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// helperEnv makes the test binary act as a child command instead of running tests.
const helperEnv = "DISPATCH_E2E_HELPER"

var dispatchBin string

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "1" {
		os.Exit(helper(os.Args[1:]))
	}
	dir, err := os.MkdirTemp("", "dispatch-e2e-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dispatchBin = filepath.Join(dir, "dispatch")
	build := exec.Command("go", "build", "-o", dispatchBin, "../cmd/dispatch")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building dispatch:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// helper implements the child commands the tests run.
func helper(args []string) int {
	if len(args) == 0 {
		return 2
	}
	n := func(i int) int {
		v, _ := strconv.Atoi(args[i])
		return v
	}
	switch args[0] {
	case "lines": // lines N: N numbered lines on stdout, every 10th also on stderr
		for i := 1; i <= n(1); i++ {
			fmt.Fprintf(os.Stdout, "out %d\n", i)
			if i%10 == 0 {
				fmt.Fprintf(os.Stderr, "err %d\n", i)
			}
		}
	case "exit": // exit CODE: a line on each stream, then exit
		fmt.Println("about to fail")
		fmt.Fprintln(os.Stderr, "error: something broke")
		return n(1)
	case "sleep": // sleep SECONDS [MARKER]: print a marker, then sleep
		if len(args) > 2 {
			fmt.Println(args[2])
		}
		time.Sleep(time.Duration(n(1)) * time.Second)
		fmt.Println("woke")
	case "ticker": // ticker N MS: N lines, one every MS milliseconds
		for i := 1; i <= n(1); i++ {
			fmt.Printf("tick %d\n", i)
			time.Sleep(time.Duration(n(2)) * time.Millisecond)
		}
	case "partial": // partial: write a line without newline, sleep, finish it
		fmt.Print("half")
		time.Sleep(1500 * time.Millisecond)
		fmt.Println(" done")
		fmt.Print("tail-without-newline")
	case "args": // args ...: print each argument as JSON
		data, _ := json.Marshal(args[1:])
		fmt.Println(string(data))
	case "env": // env: print the environment as a JSON list
		data, _ := json.Marshal(os.Environ())
		fmt.Println(string(data))
	case "binary": // binary: write every byte value 0..255 to stdout
		b := make([]byte, 256)
		for i := range b {
			b[i] = byte(i)
		}
		_, _ = os.Stdout.Write(b)
	case "flood": // flood BYTES: write BYTES bytes of 'x' lines to stdout
		line := []byte(strings.Repeat("x", 99) + "\n")
		for left := n(1); left > 0; left -= len(line) {
			_, _ = os.Stdout.Write(line[:min(left, len(line))])
		}
	case "spawn": // spawn: start a grandchild in the same process group, print its pid, sleep
		c := exec.Command(os.Args[0], "sleep", "300")
		c.Env = os.Environ()
		if err := c.Start(); err != nil {
			return 3
		}
		fmt.Printf("grandchild %d\n", c.Process.Pid)
		time.Sleep(300 * time.Second)
	case "ignore-term": // ignore-term: ignore SIGTERM and sleep
		signalIgnoreTerm()
		fmt.Println("ignoring SIGTERM")
		time.Sleep(300 * time.Second)
	case "background": // background: leave a grandchild running and exit at once
		c := exec.Command(os.Args[0], "sleep", "300")
		c.Env = os.Environ()
		c.Stdout, c.Stderr = nil, nil
		if err := c.Start(); err != nil {
			return 3
		}
		fmt.Printf("grandchild %d\n", c.Process.Pid)
	default:
		return 2
	}
	return 0
}

type harness struct {
	t        *testing.T
	cs       *mcp.ClientSession
	cmd      *exec.Cmd
	stateDir string
}

func newHarness(t *testing.T, stateDir string, flags ...string) *harness {
	t.Helper()
	if stateDir == "" {
		stateDir = t.TempDir()
	}
	cmd := exec.Command(dispatchBin, append([]string{"-state-dir", stateDir}, flags...)...)
	cmd.Stderr = os.Stderr
	client := mcp.NewClient(&mcp.Implementation{Name: "dispatch-e2e", Version: "0"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cs, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("connecting to dispatch: %v", err)
	}
	h := &harness{t: t, cs: cs, cmd: cmd, stateDir: stateDir}
	t.Cleanup(func() { _ = cs.Close() })
	return h
}

// call invokes a tool and decodes its structured result into out. It fails
// the test on a tool error unless wantErr is set, and returns the error text.
func (h *harness) call(name string, args map[string]any, out any) string {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	res, err := h.cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		h.t.Fatalf("%s: %v", name, err)
	}
	if res.IsError {
		var texts []string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				texts = append(texts, tc.Text)
			}
		}
		return strings.Join(texts, "\n")
	}
	data, err := json.Marshal(res.StructuredContent)
	if err != nil {
		h.t.Fatalf("%s: %v", name, err)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			h.t.Fatalf("%s: decoding %s: %v", name, data, err)
		}
	}
	return ""
}

func (h *harness) mustCall(name string, args map[string]any, out any) {
	h.t.Helper()
	if msg := h.call(name, args, out); msg != "" {
		h.t.Fatalf("%s returned a tool error: %s", name, msg)
	}
}

// helperCommand returns run_command arguments that run the test binary in helper mode.
func helperCommand(args ...string) map[string]any {
	return map[string]any{
		"command": os.Args[0],
		"args":    args,
		"env":     map[string]any{"set": map[string]string{helperEnv: "1"}},
	}
}

func with(m map[string]any, kv ...any) map[string]any {
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func processExists(pid int) bool { return syscall.Kill(pid, 0) == nil }

func waitGone(pid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for processExists(pid) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}
