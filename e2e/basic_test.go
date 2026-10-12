package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type outLine struct {
	N            int64  `json:"n"`
	Stream       string `json:"stream"`
	Text         string `json:"text"`
	Match        bool   `json:"match"`
	Partial      bool   `json:"partial"`
	Unterminated bool   `json:"unterminated"`
	Truncated    bool   `json:"truncated"`
	Bytes        int64  `json:"bytes"`
	ByteOffset   *int64 `json:"byte_offset"`
	InvalidUTF8  bool   `json:"invalid_utf8"`
}

type streamStatus struct {
	Bytes            int64  `json:"bytes"`
	Lines            int64  `json:"lines"`
	LostBytes        *int64 `json:"lost_bytes"`
	PartialLineBytes int64  `json:"partial_line_bytes"`
}

type status struct {
	RunID          string   `json:"run_id"`
	State          string   `json:"state"`
	Label          string   `json:"label"`
	LabelTruncated bool     `json:"label_truncated"`
	Command        string   `json:"command"`
	Executable     string   `json:"executable"`
	Args           []string `json:"args"`
	ArgsTruncated  bool     `json:"args_truncated"`
	Cwd            string   `json:"cwd"`
	Env            struct {
		Inherit bool     `json:"inherit"`
		SetKeys []string `json:"set_keys"`
		Unset   []string `json:"unset"`
	} `json:"env"`
	PID          int     `json:"pid"`
	StartedAt    string  `json:"started_at"`
	EndedAt      string  `json:"ended_at"`
	ExitCode     *int    `json:"exit_code"`
	Signal       string  `json:"signal"`
	Termination  string  `json:"termination"`
	Error        string  `json:"error"`
	ProcessGroup string  `json:"process_group"`
	Duration     float64 `json:"duration_seconds"`
	Output       struct {
		CaptureActive bool         `json:"capture_active"`
		Complete      bool         `json:"complete"`
		Stdout        streamStatus `json:"stdout"`
		Stderr        streamStatus `json:"stderr"`
		CombinedLines int64        `json:"combined_lines"`
		LimitBytes    int64        `json:"limit_bytes"`
		LimitReached  *bool        `json:"limit_reached"`
		StatsUnknown  bool         `json:"stats_unknown"`
		CaptureError  string       `json:"capture_error"`
	} `json:"output"`
	Wait *struct {
		Finished        bool    `json:"finished"`
		DeadlineReached bool    `json:"deadline_reached"`
		WaitedSeconds   float64 `json:"waited_seconds"`
	} `json:"wait"`
	Tail *struct {
		Lines     []outLine `json:"lines"`
		Truncated bool      `json:"truncated"`
	} `json:"tail"`
}

type captureSummary struct {
	Active       bool   `json:"active"`
	Complete     bool   `json:"complete"`
	LostBytes    *int64 `json:"lost_bytes"`
	LimitReached *bool  `json:"limit_reached"`
	StatsUnknown bool   `json:"stats_unknown"`
	Error        string `json:"error"`
}

type readResult struct {
	Stream            string         `json:"stream"`
	State             string         `json:"state"`
	Lines             []outLine      `json:"lines"`
	FirstLine         int64          `json:"first_line"`
	LastLine          int64          `json:"last_line"`
	TotalLines        int64          `json:"total_lines"`
	NextCursor        string         `json:"next_cursor"`
	MoreAvailable     bool           `json:"more_available"`
	ResponseTruncated bool           `json:"response_truncated"`
	Data              *string        `json:"data"`
	Encoding          string         `json:"encoding"`
	NextByteOffset    *int64         `json:"next_byte_offset"`
	TotalBytes        *int64         `json:"total_bytes"`
	InvalidUTF8       bool           `json:"invalid_utf8"`
	Capture           captureSummary `json:"capture"`
}

func texts(lines []outLine) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.Text
	}
	return out
}

// mustLostBytes and mustLimitReached unwrap a known lost_bytes/limit_reached
// value, failing the test if it is unknown (nil) where the test expects an
// authoritative answer (the run is not owned by another live instance).
func mustLostBytes(t *testing.T, p *int64) int64 {
	t.Helper()
	if p == nil {
		t.Fatal("lost_bytes is unknown (nil), want a known value")
	}
	return *p
}

func mustLimitReached(t *testing.T, p *bool) bool {
	t.Helper()
	if p == nil {
		t.Fatal("limit_reached is unknown (nil), want a known value")
	}
	return *p
}

func TestRunWaitAndRead(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("lines", "100"), "wait", true), &st)
	if st.State != "exited" || st.ExitCode == nil || *st.ExitCode != 0 || st.Termination != "exit" {
		t.Fatalf("status = %+v", st)
	}
	if !st.Wait.Finished || st.Wait.DeadlineReached {
		t.Fatalf("wait = %+v", st.Wait)
	}
	if st.Output.Stdout.Lines != 100 || st.Output.Stderr.Lines != 10 || st.Output.CombinedLines != 110 || !st.Output.Complete {
		t.Fatalf("output = %+v", st.Output)
	}
	if st.Tail == nil || len(st.Tail.Lines) != 10 || st.Tail.Lines[9].Text != "err 100" && st.Tail.Lines[9].Text != "out 100" {
		t.Fatalf("tail = %+v", st.Tail)
	}

	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "start_line": 41, "max_lines": 5}, &rr)
	if got := strings.Join(texts(rr.Lines), ","); got != "out 41,out 42,out 43,out 44,out 45" {
		t.Fatalf("lines = %s", got)
	}
	if !rr.MoreAvailable || rr.NextCursor != "stdout:46" || rr.TotalLines != 100 || !rr.Capture.Complete {
		t.Fatalf("read = %+v", rr)
	}
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stderr", "start_line": -2}, &rr)
	if got := strings.Join(texts(rr.Lines), ","); got != "err 90,err 100" || rr.MoreAvailable {
		t.Fatalf("stderr tail = %s %+v", got, rr)
	}
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "max_lines": 2000}, &rr)
	if rr.Stream != "combined" || len(rr.Lines) != 110 || rr.MoreAvailable {
		t.Fatalf("combined = %d lines, more=%v", len(rr.Lines), rr.MoreAvailable)
	}
	// Each stream's own order is exact inside the combined view; the order across
	// streams is only the order Dispatch received them, so it is not asserted.
	next := map[string]int{"stdout": 1, "stderr": 10}
	for _, l := range rr.Lines {
		want := fmt.Sprintf("out %d", next["stdout"])
		if l.Stream == "stderr" {
			want = fmt.Sprintf("err %d", next["stderr"])
		}
		if l.Text != want {
			t.Fatalf("combined line %d = %s %q, want %q", l.N, l.Stream, l.Text, want)
		}
		if l.Stream == "stderr" {
			next["stderr"] += 10
		} else {
			next["stdout"]++
		}
	}
}

func TestWaitFalseThenWaitForRun(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("sleep", "2", "started"), "wait", false), &st)
	if st.State != "running" || st.Wait != nil || st.RunID == "" {
		t.Fatalf("status = %+v", st)
	}
	var w status
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID, "timeout_seconds": 0.3}, &w)
	if w.State != "running" || !w.Wait.DeadlineReached || w.Wait.Finished {
		t.Fatalf("short wait = %+v", w.Wait)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID, "timeout_seconds": 30}, &w)
	if w.State != "exited" || !w.Wait.Finished || *w.ExitCode != 0 {
		t.Fatalf("wait = %+v", w)
	}
	if got := texts(w.Tail.Lines); strings.Join(got, ",") != "started,woke" {
		t.Fatalf("tail = %v", got)
	}
}

func TestRunCommandWaitDeadline(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("sleep", "3"), "wait", true, "wait_timeout_seconds", 0.5), &st)
	if st.State != "running" || !st.Wait.DeadlineReached || st.Wait.WaitedSeconds > 2.5 {
		t.Fatalf("status = %+v wait = %+v", st, st.Wait)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID, "timeout_seconds": 30}, &st)
	if st.State != "exited" || *st.ExitCode != 0 {
		t.Fatalf("run did not continue after the wait deadline: %+v", st)
	}
}

func TestCommandFailure(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("exit", "7"), "wait", true), &st)
	if st.State != "exited" || st.ExitCode == nil || *st.ExitCode != 7 || st.Termination != "exit" {
		t.Fatalf("status = %+v", st)
	}
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stderr"}, &rr)
	if len(rr.Lines) != 1 || rr.Lines[0].Text != "error: something broke" {
		t.Fatalf("stderr = %+v", rr.Lines)
	}
}

func TestFailedToStart(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", map[string]any{"command": "dispatch-no-such-command-xyz", "wait": true}, &st)
	if st.State != "failed_to_start" || !strings.Contains(st.Error, "not found") || st.RunID == "" {
		t.Fatalf("status = %+v", st)
	}
	h.mustCall("run_command", map[string]any{"command": "/bin/sh", "cwd": "/no/such/dir", "wait": false}, &st)
	if st.State != "failed_to_start" || !strings.Contains(st.Error, "working directory") {
		t.Fatalf("status = %+v", st)
	}
	h.mustCall("get_status", map[string]any{"run_id": st.RunID}, &st)
	if st.State != "failed_to_start" {
		t.Fatalf("get_status = %+v", st)
	}
}

func TestArgsPreservedExactly(t *testing.T) {
	h := newHarness(t, "")
	args := []string{"", "two words", `"quoted"`, "$HOME", "*", "semi;colon", "new\nline", "ünïcødé", "-flag=value with spaces", "back\\slash"}
	var st status
	h.mustCall("run_command", with(helperCommand(append([]string{"args"}, args...)...), "wait", true), &st)
	var rr readResult
	h.mustCall("read_output", map[string]any{"run_id": st.RunID, "stream": "stdout"}, &rr)
	var got []string
	if len(rr.Lines) != 1 || json.Unmarshal([]byte(rr.Lines[0].Text), &got) != nil {
		t.Fatalf("output = %+v", rr.Lines)
	}
	if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", args) {
		t.Fatalf("args = %q, want %q", got, args)
	}
	if fmt.Sprintf("%q", st.Args[1:]) != fmt.Sprintf("%q", args) {
		t.Fatalf("status args = %q", st.Args)
	}
}
