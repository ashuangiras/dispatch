package e2e

import (
	"strings"
	"testing"
	"time"
)

func TestRunOwnedByAnotherLiveInstance(t *testing.T) {
	stateDir := t.TempDir()
	a := newHarness(t, stateDir)
	b := newHarness(t, stateDir)
	var st status
	a.mustCall("run_command", with(helperCommand("sleep", "30", "owned-by-a"), "wait", true, "wait_timeout_seconds", 0.5), &st)

	var seen struct {
		status
		OwnedElsewhere bool `json:"owned_elsewhere"`
	}
	b.mustCall("get_status", map[string]any{"run_id": st.RunID}, &seen)
	if seen.State != "running" || !seen.OwnedElsewhere || !seen.Output.CaptureActive {
		t.Fatalf("status from other instance = %+v", seen)
	}
	var rr readResult
	b.mustCall("read_output", map[string]any{"run_id": st.RunID}, &rr)
	if len(rr.Lines) != 1 || rr.Lines[0].Text != "owned-by-a" {
		t.Fatalf("read from other instance = %+v", rr.Lines)
	}
	if msg := b.call("cancel_run", map[string]any{"run_id": st.RunID}, nil); !strings.Contains(msg, "another live Dispatch instance") {
		t.Fatalf("cancel from other instance = %q", msg)
	}
	var w status
	b.mustCall("wait_for_run", map[string]any{"run_id": st.RunID, "timeout_seconds": 0.5}, &w)
	if w.State != "running" || !w.Wait.DeadlineReached {
		t.Fatalf("wait from other instance = %+v", w.Wait)
	}
	a.mustCall("cancel_run", map[string]any{"run_id": st.RunID}, &st)
	b.mustCall("wait_for_run", map[string]any{"run_id": st.RunID, "timeout_seconds": 10}, &w)
	if w.State != "exited" || w.Termination != "cancelled" || !w.Wait.Finished {
		t.Fatalf("status after owner cancelled = %+v", w)
	}
}

func TestRetentionRemovesOldestFinishedRuns(t *testing.T) {
	h := newHarness(t, "", "-max-retained-runs", "2")
	var running status
	h.mustCall("run_command", with(helperCommand("sleep", "30"), "wait", false), &running)
	var ids []string
	for i := 0; i < 4; i++ {
		var st status
		h.mustCall("run_command", with(helperCommand("lines", "5"), "wait", true), &st)
		ids = append(ids, st.RunID)
	}
	var lr struct {
		Runs []struct {
			RunID string `json:"run_id"`
		} `json:"runs"`
		Total int `json:"total"`
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.mustCall("list_runs", map[string]any{}, &lr)
		if lr.Total <= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	got := map[string]bool{}
	for _, r := range lr.Runs {
		got[r.RunID] = true
	}
	// The running run is never removed; the newest finished run is kept.
	if lr.Total != 2 || !got[running.RunID] || !got[ids[3]] {
		t.Fatalf("retained = %+v, want the running run and %s", lr.Runs, ids[3])
	}
	h.mustCall("cancel_run", map[string]any{"run_id": running.RunID}, nil)
}
