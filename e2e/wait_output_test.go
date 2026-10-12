package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type outputWaitResult struct {
	Outcome    string `json:"outcome"`
	NextCursor string `json:"next_cursor"`
	Search     struct {
		Matches     int            `json:"matches"`
		ScannedFrom int64          `json:"scanned_from"`
		State       string         `json:"state"`
		Capture     captureSummary `json:"capture"`
		Groups      []struct {
			Lines []outLine `json:"lines"`
		} `json:"groups"`
	} `json:"search"`
}

func TestWaitForOutputLiveMatchAndCursor(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("ticker", "5", "150"), "wait", false), &st)
	var found outputWaitResult
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "tick [23]", "regex": true, "timeout_seconds": 5, "max_bytes": 70}, &found)
	if found.Outcome != "match_found" || found.Search.Matches != 1 || found.Search.State != "running" || found.NextCursor != "combined:3" {
		t.Fatalf("first match = %+v", found)
	}
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "tick 4", "cursor": found.NextCursor, "timeout_seconds": 5}, &found)
	if found.Outcome != "match_found" || found.Search.ScannedFrom < 3 || found.NextCursor != "combined:5" {
		t.Fatalf("continued match = %+v", found)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID}, nil)
}

func TestWaitForOutputDeadlineAndCancellationLeaveRunAlone(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("sleep", "3", "ready"), "wait", false), &st)
	var found outputWaitResult
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "never", "timeout_seconds": 0.05}, &found)
	if found.Outcome != "deadline_reached" {
		t.Fatalf("outcome = %s", found.Outcome)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := h.cs.CallTool(ctx, &mcp.CallToolParams{Name: "wait_for_output", Arguments: map[string]any{"run_id": st.RunID, "pattern": "never", "timeout_seconds": 5}})
	if err == nil {
		t.Fatal("expected cancelled call to return an error")
	}
	h.mustCall("get_status", map[string]any{"run_id": st.RunID}, &st)
	if st.State != "running" {
		t.Fatalf("wait affected run: %s", st.State)
	}
	h.mustCall("cancel_run", map[string]any{"run_id": st.RunID, "grace_seconds": 0.1}, nil)
}

func TestWaitForOutputExistingMatchEndAndStreams(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("exit", "1"), "wait", true), &st)
	var found outputWaitResult
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "stream": "stderr", "pattern": "ERROR:.*BROKE", "regex": true, "ignore_case": true, "timeout_seconds": 0}, &found)
	if found.Outcome != "match_found" || found.NextCursor != "stderr:2" {
		t.Fatalf("existing match = %+v", found)
	}
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "stream": "stderr", "pattern": "error", "cursor": found.NextCursor, "timeout_seconds": 0}, &found)
	if found.Outcome != "run_ended" || !found.Search.Capture.Complete {
		t.Fatalf("after matching cursor = %+v", found)
	}
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "pattern": "something broke", "timeout_seconds": 0}, &found)
	if found.Outcome != "run_ended" {
		t.Fatalf("wrong-stream absence = %+v", found)
	}
}

func TestWaitForOutputCaptureLossAndUnknown(t *testing.T) {
	stateDir := t.TempDir()
	a := newHarness(t, stateDir)
	var st status
	a.mustCall("run_command", with(helperCommand("flood", "1000"), "wait", true, "max_output_bytes", 10), &st)
	var found outputWaitResult
	a.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "missing"}, &found)
	if found.Outcome != "capture_incomplete" || mustLostBytes(t, found.Search.Capture.LostBytes) == 0 {
		t.Fatalf("loss = %+v", found)
	}
	a.mustCall("run_command", with(helperCommand("sleep", "3", "ready"), "wait", true, "wait_timeout_seconds", 0.1), &st)
	b := newHarness(t, stateDir)
	b.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "missing"}, &found)
	if found.Outcome != "capture_incomplete" || !found.Search.Capture.StatsUnknown {
		t.Fatalf("foreign capture = %+v", found)
	}
	a.mustCall("cancel_run", map[string]any{"run_id": st.RunID, "grace_seconds": 0.1}, nil)
}

func TestWaitForOutputPartialLineAndSearchCoverage(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("partial"), "wait", false), &st)
	var found outputWaitResult
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "pattern": "half", "timeout_seconds": 0.1}, &found)
	if found.Outcome != "deadline_reached" {
		t.Fatalf("partial matched prematurely: %+v", found)
	}
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "stream": "stdout", "pattern": "half done", "cursor": found.NextCursor, "timeout_seconds": 5}, &found)
	if found.Outcome != "match_found" {
		t.Fatalf("completed line = %+v", found)
	}
	h.mustCall("wait_for_run", map[string]any{"run_id": st.RunID}, nil)
	h.mustCall("run_command", with(helperCommand("longline"), "wait", true), &st)
	h.mustCall("wait_for_output", map[string]any{"run_id": st.RunID, "pattern": "needle"}, &found)
	if found.Outcome != "search_incomplete" {
		t.Fatalf("long-line absence was trusted: %+v", found)
	}
}

func TestWaitForOutputValidation(t *testing.T) {
	h := newHarness(t, "")
	var st status
	h.mustCall("run_command", with(helperCommand("exit", "0"), "wait", true), &st)
	for _, args := range []map[string]any{
		{"pattern": "[", "regex": true}, {"pattern": ""}, {"pattern": "x", "timeout_seconds": -1},
		{"pattern": "x", "timeout_seconds": 301}, {"pattern": "x", "stream": "bad"},
		{"pattern": "x", "cursor": "stderr:1", "stream": "stdout"}, {"pattern": "x", "max_bytes": 0},
	} {
		args["run_id"] = st.RunID
		if msg := h.call("wait_for_output", args, nil); strings.TrimSpace(msg) == "" {
			t.Errorf("expected validation error for %+v", args)
		}
	}
}
