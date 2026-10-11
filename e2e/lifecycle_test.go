package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type searchResult struct {
	Stream string `json:"stream"`
	Groups []struct {
		Lines []outLine `json:"lines"`
	} `json:"groups"`
	Matches                    int            `json:"matches"`
	ScannedFrom                int64          `json:"scanned_from"`
	ScannedThrough             int64          `json:"scanned_through"`
	TotalLines                 int64          `json:"total_lines"`
	ScanComplete               bool           `json:"scan_complete"`
	NextCursor                 string         `json:"next_cursor"`
	MoreAvailable              bool           `json:"more_available"`
	ResponseTruncated          bool           `json:"response_truncated"`
	LongLinesPartiallySearched int            `json:"long_lines_partially_searched"`
	Capture                    captureSummary `json:"capture"`
}

func TestHighVolumeOutput(t *testing.T) {
	h := newHarness(t, "")
	const n = 200000
	var st status
	h.mustCall("run_command", with(helperCommand("lines", strconv.Itoa(n)), "wait", true, "wait_timeout_seconds", 120), &st)
	if st.State != "exited" || st.Output.Stdout.Lines != n || st.Output.Stderr.Lines != n/10 || !st.Output.Complete {
		t.Fatalf("status = %+v", st.Output)
	}
	wantBytes := int64(0)
	for i := 1; i <= n; i++ {
		wantBytes += int64(len(fmt.Sprintf("out %d\n", i)))
	}
	if st.Output.Stdout.Bytes != wantBytes {
		t.Fatalf("stdout bytes = %d, want %d", st.Output.Stdout.Bytes, wantBytes)
	}
	if len(st.Tail.Lines) != 10 {
		t.Fatalf("tail = %d lines", len(st.Tail.Lines))
	}
	// A default read is bounded, says more is available, and is not mistaken for loss.
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout"}, &rr)
	if len(rr.Lines) != 200 || !rr.MoreAvailable || !rr.ResponseTruncated || rr.Capture.LostBytes != 0 || !rr.Capture.Complete {
		t.Fatalf("read = %d lines more=%v trunc=%v capture=%+v", len(rr.Lines), rr.MoreAvailable, rr.ResponseTruncated, rr.Capture)
	}
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "start_line": 150000, "max_lines": 2000, "max_bytes": 1000}, &rr)
	if rr.Lines[0].Text != "out 150000" || len(rr.Lines) >= 2000 || !rr.ResponseTruncated {
		t.Fatalf("byte-bounded read = %d lines, first %q", len(rr.Lines), rr.Lines[0].Text)
	}
	next := rr.NextCursor
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "cursor": next, "max_lines": 1}, &rr)
	if want := fmt.Sprintf("out %d", 150000+int(rr.FirstLine-150000)); rr.Lines[0].Text != want || rr.Stream != "stdout" {
		t.Fatalf("cursor read = %+v", rr.Lines)
	}
	var sr searchResult
	h.mustCall("search_output", map[string]any{"run_id": st.RunID, "pattern": `^err 1999[0-9]0$`, "regex": true, "stream": "stderr", "context_lines": 0}, &sr)
	if sr.Matches != 10 || !sr.ScanComplete || sr.MoreAvailable {
		t.Fatalf("search = %+v", sr)
	}
}

func TestIncrementalReadsWhileRunning(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("ticker", "30", "50"), "wait", false), &st)
	var seen []string
	cursor := "stdout:1"
	deadline := time.Now().Add(30 * time.Second)
	for {
		var rr readResult
		h.mustCall("read_output", map[string]any{"run_id": st.RunID, "cursor": cursor}, &rr)
		for _, l := range rr.Lines {
			if !l.Partial {
				seen = append(seen, l.Text)
			}
		}
		cursor = rr.NextCursor
		if rr.State != "running" && !rr.MoreAvailable && !rr.Capture.Active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not finish")
		}
		time.Sleep(70 * time.Millisecond)
	}
	if len(seen) != 30 {
		t.Fatalf("saw %d lines: %v", len(seen), seen)
	}
	for i, s := range seen {
		if s != fmt.Sprintf("tick %d", i+1) {
			t.Fatalf("line %d = %q (skipped or repeated)", i+1, s)
		}
	}
}

func TestPartialLineAndUnterminatedEnd(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("partial"), "wait", true, "wait_timeout_seconds", 0.7), &st)
	if st.State != "running" {
		t.Fatalf("state = %s", st.State)
	}
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout"}, &rr)
	if len(rr.Lines) != 1 || !rr.Lines[0].Partial || rr.Lines[0].Text != "half" || rr.TotalLines != 0 || rr.NextCursor != "stdout:1" {
		t.Fatalf("partial read = %+v", rr)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID}, &st)
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "cursor": rr.NextCursor}, &rr)
	if got := strings.Join(texts(rr.Lines), "|"); got != "half done|tail-without-newline" {
		t.Fatalf("lines = %q", got)
	}
	if rr.Lines[0].Partial || rr.Lines[0].Unterminated || !rr.Lines[1].Unterminated {
		t.Fatalf("flags = %+v", rr.Lines)
	}
}

func TestSearchWithContextAndPaging(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("lines", "1000"), "wait", true), &st)
	var sr searchResult
	h.mustCall("search_output", map[string]any{"run_id": st.RunID, "pattern": "OUT 5", "ignore_case": true, "stream": "stdout", "max_matches": 3, "context_lines": 1}, &sr)
	// out 5, out 50, out 51 match; contexts of 50 and 51 merge.
	if sr.Matches != 3 || len(sr.Groups) != 2 || sr.ScanComplete || !sr.MoreAvailable {
		t.Fatalf("search = %+v", sr)
	}
	if got := strings.Join(texts(sr.Groups[1].Lines), ","); got != "out 49,out 50,out 51,out 52" {
		t.Fatalf("group = %s", got)
	}
	if !sr.Groups[0].Lines[1].Match || sr.Groups[0].Lines[0].Match {
		t.Fatalf("match flags = %+v", sr.Groups[0].Lines)
	}
	total := sr.Matches
	for sr.MoreAvailable {
		h.mustCall("search_output", map[string]any{"run_id": st.RunID, "pattern": "out 5", "cursor": sr.NextCursor, "max_matches": 50, "context_lines": 0}, &sr)
		total += sr.Matches
	}
	// out 5, 50-59, 500-599
	if total != 1+10+100 {
		t.Fatalf("total matches = %d", total)
	}
	if msg := h.call("search_output", map[string]any{"run_id": st.RunID, "pattern": "(", "regex": true}, nil); !strings.Contains(msg, "invalid regex") {
		t.Fatalf("bad regex error = %q", msg)
	}
}

func TestConcurrentRuns(t *testing.T) {
	h := newHarness(t, "")
	const k = 12
	ids := make([]string, k)
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var st status
			h.mustCall("run_command", with(helperCommand("lines", strconv.Itoa(1000*(i+1))), "wait", false, "label", fmt.Sprintf("job-%d", i)), &st)
			ids[i] = st.RunID
		}(i)
	}
	wg.Wait()
	for i, id := range ids {
		var st status
		h.mustCall("wait_for_run", map[string]any{"run_id": id, "timeout_seconds": 120, "tail_lines": 1}, &st)
		if st.State != "exited" || *st.ExitCode != 0 || st.Output.Stdout.Lines != int64(1000*(i+1)) || !st.Output.Complete {
			t.Fatalf("run %d = %+v", i, st.Output)
		}
	}
	var lr struct {
		Runs []struct {
			RunID string `json:"run_id"`
			Label string `json:"label"`
		} `json:"runs"`
		Total      int    `json:"total"`
		NextCursor string `json:"next_cursor"`
	}
	h.mustCall("list_runs", map[string]any{"limit": 5}, &lr)
	if lr.Total != k || len(lr.Runs) != 5 || lr.NextCursor != "5" {
		t.Fatalf("list = %+v", lr)
	}
}

func TestMaxActiveRuns(t *testing.T) {
	h := newHarness(t, "", "-max-active", "2")
	var a, b status
	h.mustCall("run_command", with(helperCommand("sleep", "30"), "wait", false), &a)
	h.mustCall("run_command", with(helperCommand("sleep", "30"), "wait", false), &b)
	if msg := h.call("run_command", with(helperCommand("sleep", "1"), "wait", false), nil); !strings.Contains(msg, "too many active runs") {
		t.Fatalf("third run error = %q", msg)
	}
	h.mustCall("cancel_run", map[string]any{"run_id": a.RunID}, &a)
	h.mustCall("run_command", with(helperCommand("exit", "0"), "wait", true), nil)
	h.mustCall("cancel_run", map[string]any{"run_id": b.RunID}, nil)
}

func TestExecutionTimeoutKillsProcessGroup(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("spawn"), "wait", true, "timeout_seconds", 1, "wait_timeout_seconds", 30), &st)
	if st.State != "exited" || st.Termination != "timeout" || st.Signal != "SIGTERM" {
		t.Fatalf("status = %+v", st)
	}
	gc := grandchildPID(t, h, st.RunID)
	if !waitGone(gc, 5*time.Second) {
		_ = syscall.Kill(gc, syscall.SIGKILL)
		t.Fatalf("grandchild %d survived the timeout", gc)
	}
}

func TestTimeoutEscalatesToSIGKILL(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("ignore-term"), "wait", true, "timeout_seconds", 0.5, "kill_grace_seconds", 0.5), &st)
	if st.State != "exited" || st.Termination != "timeout" || st.Signal != "SIGKILL" {
		t.Fatalf("status = %+v", st)
	}
}

func TestCancelKillsDescendants(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("spawn"), "wait", true, "wait_timeout_seconds", 1), &st)
	if st.State != "running" {
		t.Fatalf("state = %s", st.State)
	}
	gc := grandchildPID(t, h, st.RunID)
	if !processExists(gc) {
		t.Fatalf("grandchild %d not running before cancel", gc)
	}
	h.mustCall("cancel_run", map[string]any{"run_id": st.RunID, "grace_seconds": 2}, &st)
	if st.State != "exited" || st.Termination != "cancelled" {
		t.Fatalf("status = %+v", st)
	}
	if !waitGone(gc, 5*time.Second) {
		_ = syscall.Kill(gc, syscall.SIGKILL)
		t.Fatalf("grandchild %d survived cancel", gc)
	}
	// Cancelling again changes nothing.
	var again status
	h.mustCall("cancel_run", map[string]any{"run_id": st.RunID}, &again)
	if again.Termination != "cancelled" || again.EndedAt != st.EndedAt {
		t.Fatalf("second cancel = %+v", again)
	}
}

func TestLeftoverGroupMembersAreTerminated(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("background"), "wait", true), &st)
	if st.State != "exited" || *st.ExitCode != 0 || !strings.Contains(st.ProcessGroup, "terminated") {
		t.Fatalf("status = %+v", st)
	}
	gc := grandchildPID(t, h, st.RunID)
	if !waitGone(gc, 5*time.Second) {
		_ = syscall.Kill(gc, syscall.SIGKILL)
		t.Fatalf("background grandchild %d survived the run", gc)
	}
}

func grandchildPID(t *testing.T, h *harness, runID string) int {
	t.Helper()
	var sr searchResult
	h.mustCall("search_output", map[string]any{"run_id": runID, "pattern": "grandchild ", "context_lines": 0}, &sr)
	if sr.Matches != 1 {
		t.Fatalf("grandchild line not found: %+v", sr)
	}
	pid, err := strconv.Atoi(strings.TrimPrefix(sr.Groups[0].Lines[0].Text, "grandchild "))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestCaptureLimitIsReported(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("flood", "1000000"), "wait", true, "max_output_bytes", 65536), &st)
	if st.State != "exited" || *st.ExitCode != 0 {
		t.Fatalf("the command must finish normally when capture is limited: %+v", st)
	}
	o := st.Output
	if !o.LimitReached || o.Stdout.Bytes != 65536 || o.Stdout.LostBytes != 1000000-65536 || o.Complete || o.LimitBytes != 65536 {
		t.Fatalf("output = %+v", o)
	}
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "start_line": -1}, &rr)
	if !rr.Capture.LimitReached || rr.Capture.LostBytes != 1000000-65536 || rr.Capture.Complete || !rr.Lines[0].Unterminated {
		t.Fatalf("read = %+v", rr)
	}
	if msg := h.call("run_command", with(helperCommand("exit", "0"), "wait", false, "max_output_bytes", int64(1)<<40), nil); !strings.Contains(msg, "max_output_bytes") {
		t.Fatalf("over-limit request error = %q", msg)
	}
}

func TestBinaryOutputAndByteReads(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("binary"), "wait", true), &st)
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "byte_offset": 0, "encoding": "base64"}, &rr)
	data, err := base64.StdEncoding.DecodeString(*rr.Data)
	if err != nil || len(data) != 256 || *rr.TotalBytes != 256 || rr.MoreAvailable {
		t.Fatalf("bytes = %d %v %+v", len(data), err, rr)
	}
	for i, b := range data {
		if int(b) != i {
			t.Fatalf("byte %d = %d", i, b)
		}
	}
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "start_line": 2}, &rr)
	if len(rr.Lines) != 1 || !rr.Lines[0].InvalidUTF8 || !rr.Lines[0].Unterminated {
		t.Fatalf("text line = %+v", rr.Lines)
	}
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "byte_offset": 250, "max_bytes": 4}, &rr)
	if *rr.NextByteOffset != 254 || !rr.MoreAvailable {
		t.Fatalf("byte page = %+v", rr)
	}
}

func TestEnvironmentPrecedenceAndSecrecy(t *testing.T) {
	t.Setenv("DISPATCH_E2E_INHERITED", "from-server")
	t.Setenv("DISPATCH_E2E_REMOVED", "gone")
	h := newHarness(t, "")
	envOf := func(st status) map[string]string {
		// Read all output bytes to handle large environments that exceed line length limits.
		var rr readResult
		var fullData []byte
		offset := int64(0)
		for {
			h.mustCall("read_output", map[string]any{
				"run_id":      st.RunID,
				"stream":      "stdout",
				"byte_offset": offset,
				"encoding":    "base64",
				"max_bytes":   65536,
			}, &rr)
			if rr.Data != nil {
				data, err := base64.StdEncoding.DecodeString(*rr.Data)
				if err != nil {
					t.Fatal(err)
				}
				fullData = append(fullData, data...)
			}
			if !rr.MoreAvailable {
				break
			}
			if rr.NextByteOffset != nil {
				offset = *rr.NextByteOffset
			}
		}
		var kv []string
		if err := json.Unmarshal(fullData, &kv); err != nil {
			t.Fatal(err)
		}
		m := map[string]string{}
		for _, e := range kv {
			k, v, _ := strings.Cut(e, "=")
			m[k] = v
		}
		return m
	}
	args := helperCommand("env")
	args["env"] = map[string]any{
		"set":   map[string]string{helperEnv: "1", "DISPATCH_E2E_SECRET": "s3cr3t-value", "DISPATCH_E2E_INHERITED": "overridden"},
		"unset": []string{"DISPATCH_E2E_REMOVED"},
	}
	var st status
	h.mustCall("run_command", with(args, "wait", true), &st)
	env := envOf(st)
	if env["DISPATCH_E2E_SECRET"] != "s3cr3t-value" || env["DISPATCH_E2E_INHERITED"] != "overridden" || env["PATH"] == "" {
		t.Fatalf("env = %v", env)
	}
	if _, ok := env["DISPATCH_E2E_REMOVED"]; ok {
		t.Fatal("unset variable reached the child")
	}
	if !st.Env.Inherit || !slices.Contains(st.Env.SetKeys, "DISPATCH_E2E_SECRET") || !slices.Equal(st.Env.Unset, []string{"DISPATCH_E2E_REMOVED"}) {
		t.Fatalf("env record = %+v", st.Env)
	}
	// Values never appear in run metadata, in responses or on disk. (The tail is
	// the child's own output, which printed its environment on purpose.)
	st.Tail = nil
	raw, _ := json.Marshal(st)
	rec, _ := os.ReadFile(h.stateDir + "/runs/" + st.RunID + "/run.json")
	if strings.Contains(string(raw), "s3cr3t-value") || strings.Contains(string(rec), "s3cr3t-value") || len(rec) == 0 {
		t.Fatal("secret value exposed")
	}

	// inherit false: only what is set.
	args = helperCommand("env")
	args["env"] = map[string]any{"inherit": false, "set": map[string]string{helperEnv: "1", "ONLY": "this"}}
	h.mustCall("run_command", with(args, "wait", true), &st)
	if env := envOf(st); len(env) != 2 || env["ONLY"] != "this" {
		t.Fatalf("empty-base env = %v", env)
	}
	// A bare command name cannot be found without PATH in the child.
	h.mustCall("run_command", map[string]any{"command": "sh", "wait": true, "env": map[string]any{"inherit": false}}, &st)
	if st.State != "failed_to_start" || !strings.Contains(st.Error, "no PATH") {
		t.Fatalf("status = %+v", st)
	}
	msg := h.call("run_command", map[string]any{"command": "sh", "wait": true, "env": map[string]any{"set": map[string]string{"A": "1"}, "unset": []string{"A"}}}, nil)
	if !strings.Contains(msg, "both set and unset") {
		t.Fatalf("conflict error = %q", msg)
	}
	// The server's own environment was not changed by any child's overrides.
	if os.Getenv("DISPATCH_E2E_INHERITED") != "from-server" {
		t.Fatal("server environment changed")
	}
}

func TestLargeEnvironmentOutput(t *testing.T) {
	// Regression test for large environments: verify that output exceeding default line limits
	// can still be completely read and parsed by byte offset reads.
	h := newHarness(t, "")

	// Create a large environment variable (> 4096 bytes) to test line truncation handling.
	largeValue := strings.Repeat("x", 5000)
	args := helperCommand("env")
	args["env"] = map[string]any{
		"set": map[string]string{
			helperEnv:        "1",
			"LARGE_ENV_TEST": largeValue,
		},
	}
	var st status
	h.mustCall("run_command", with(args, "wait", true), &st)

	// Read environment using byte offset to get complete output despite line length.
	// This replicates the envOf logic from TestEnvironmentPrecedenceAndSecrecy.
	var rr readResult
	var fullData []byte
	offset := int64(0)
	for {
		h.mustCall("read_output", map[string]any{
			"run_id":      st.RunID,
			"stream":      "stdout",
			"byte_offset": offset,
			"encoding":    "base64",
			"max_bytes":   65536,
		}, &rr)
		if rr.Data != nil {
			data, err := base64.StdEncoding.DecodeString(*rr.Data)
			if err != nil {
				t.Fatal(err)
			}
			fullData = append(fullData, data...)
		}
		if !rr.MoreAvailable {
			break
		}
		if rr.NextByteOffset != nil {
			offset = *rr.NextByteOffset
		}
	}

	var kv []string
	if err := json.Unmarshal(fullData, &kv); err != nil {
		t.Fatalf("failed to parse environment JSON: %v (data len=%d)", err, len(fullData))
	}

	// Verify the large environment value was preserved.
	found := false
	for _, e := range kv {
		k, v, _ := strings.Cut(e, "=")
		if k == "LARGE_ENV_TEST" && v == largeValue {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("large environment variable not found or truncated; env vars: %v", kv)
	}
}

func TestExplicitShell(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", map[string]any{"command": "/bin/sh", "args": []string{"-c", `echo "$0:$1" ; exit 3`, "zero", "one two"}, "wait": true}, &st)
	if *st.ExitCode != 3 || st.Tail.Lines[0].Text != "zero:one two" {
		t.Fatalf("status = %+v tail = %+v", st, st.Tail)
	}
}

func TestWaitRequestCancellationLeavesRunAlone(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("sleep", "2"), "wait", false), &st)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := h.cs.CallTool(ctx, &mcp.CallToolParams{Name: "wait_for_run", Arguments: map[string]any{"run_id": st.RunID, "timeout_seconds": 60}})
	if err == nil {
		t.Fatal("expected the cancelled request to fail")
	}
	h.mustCall("get_status", map[string]any{"run_id": st.RunID}, &st)
	if st.State != "running" {
		t.Fatalf("run state after cancelled wait = %s", st.State)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID}, &st)
	if st.State != "exited" || *st.ExitCode != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestRestartMarksInterruptedRuns(t *testing.T) {
	stateDir := t.TempDir()
	a := newHarness(t, stateDir)
	var running, done status
	a.mustCall("run_command", with(helperCommand("exit", "0"), "wait", true), &done)
	a.mustCall("run_command", with(helperCommand("sleep", "60", "before-crash"), "wait", true, "wait_timeout_seconds", 0.5), &running)
	if running.State != "running" {
		t.Fatalf("state = %s", running.State)
	}
	pid := running.PID
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
	// Crash the server: no shutdown handling runs.
	if err := a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = a.cmd.Process.Wait()

	b := newHarness(t, stateDir)
	var st status
	b.mustCall("get_status", map[string]any{"run_id": running.RunID, "tail_lines": 5}, &st)
	if st.State != "interrupted" || st.Termination != "unknown" || !strings.Contains(st.Error, "may or may not still exist") {
		t.Fatalf("status = %+v", st)
	}
	if st.Output.CaptureActive || st.Output.Complete || st.Output.CaptureError == "" {
		t.Fatalf("output = %+v", st.Output)
	}
	if len(st.Tail.Lines) != 1 || st.Tail.Lines[0].Text != "before-crash" {
		t.Fatalf("tail = %+v", st.Tail)
	}
	b.mustCall("get_status", map[string]any{"run_id": done.RunID}, &st)
	if st.State != "exited" || !st.Output.Complete {
		t.Fatalf("finished run after restart = %+v", st)
	}
	if msg := b.call("cancel_run", map[string]any{"run_id": running.RunID}, &st); msg != "" || st.State != "interrupted" {
		t.Fatalf("cancel of interrupted run = %q %+v", msg, st)
	}
}

func TestShutdownTerminatesRuns(t *testing.T) {
	stateDir := t.TempDir()
	a := newHarness(t, stateDir)
	var st status
	a.mustCall("run_command", with(helperCommand("spawn"), "wait", true, "wait_timeout_seconds", 1), &st)
	gc := grandchildPID(t, a, st.RunID)
	// Closing the client closes the server's stdin: a client disconnect.
	if err := a.cs.Close(); err != nil {
		t.Logf("close: %v", err)
	}
	if !waitGone(gc, 10*time.Second) {
		_ = syscall.Kill(gc, syscall.SIGKILL)
		t.Fatalf("grandchild %d survived server shutdown", gc)
	}
	b := newHarness(t, stateDir)
	b.mustCall("get_status", map[string]any{"run_id": st.RunID}, &st)
	if st.State != "exited" || st.Termination != "server_shutdown" {
		t.Fatalf("status = %+v", st)
	}
}

func TestDeleteRuns(t *testing.T) {
	h := newHarness(t, "")
	var a, b, r status
	h.mustCall("run_command", with(helperCommand("lines", "10"), "wait", true), &a)
	h.mustCall("run_command", with(helperCommand("lines", "10"), "wait", true), &b)
	h.mustCall("run_command", with(helperCommand("sleep", "30"), "wait", false), &r)
	var dr struct {
		Deleted    []string                         `json:"deleted"`
		Skipped    []struct{ RunID, Reason string } `json:"skipped"`
		FreedBytes int64                            `json:"freed_bytes"`
	}
	h.mustCall("delete_runs", map[string]any{"run_ids": []string{a.RunID, r.RunID, "r-20000101t000000-deadbeef", "../etc"}}, &dr)
	if !slices.Equal(dr.Deleted, []string{a.RunID}) || len(dr.Skipped) != 3 || dr.FreedBytes == 0 {
		t.Fatalf("delete = %+v", dr)
	}
	if msg := h.call("get_status", map[string]any{"run_id": a.RunID}, nil); !strings.Contains(msg, "not found") {
		t.Fatalf("deleted run status = %q", msg)
	}
	h.mustCall("delete_runs", map[string]any{"all_finished": true}, &dr)
	if !slices.Equal(dr.Deleted, []string{b.RunID}) {
		t.Fatalf("delete all finished = %+v", dr)
	}
	h.mustCall("cancel_run", map[string]any{"run_id": r.RunID}, nil)
}
