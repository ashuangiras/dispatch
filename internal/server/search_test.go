package server

import (
	"context"
	"testing"
	"time"

	"github.com/ashuangiras/dispatch/internal/runs"
)

func TestSearchContextCursorAlwaysAdvances(t *testing.T) {
	m, err := runs.New(runs.Config{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	snap, err := m.Start(runs.StartRequest{Command: "/bin/sh", Args: []string{"-c", "i=0; while [ $i -lt 8 ]; do printf 'hitxxxxxxxxxxxxxxxxx\\n'; i=$((i+1)); done"}})
	if err != nil {
		t.Fatal(err)
	}
	snap, finished, err := m.Wait(context.Background(), snap.ID, 5*time.Second)
	if err != nil || !finished {
		t.Fatalf("command did not finish: %v", err)
	}
	s := &Server{m: m}
	budget, lineBytes, contextLines := 70, 20, 2
	input := SearchInput{RunID: snap.ID, Pattern: "hit", MaxBytes: &budget, MaxLineBytes: &lineBytes, ContextLines: &contextLines}
	last := int64(0)
	for page := 0; page < 9; page++ {
		_, result, err := s.searchOutput(context.Background(), nil, input)
		if err != nil {
			t.Fatal(err)
		}
		for _, group := range result.Groups {
			for _, line := range group.Lines {
				if line.N != last+1 {
					t.Fatalf("page %d repeated or skipped a line: got %d after %d", page, line.N, last)
				}
				if !line.Match {
					t.Fatalf("matching line %d not marked as a match", line.N)
				}
				last = line.N
			}
		}
		if !result.MoreAvailable {
			if last != 8 {
				t.Fatalf("read %d lines, want 8", last)
			}
			return
		}
		if result.NextCursor == input.Cursor {
			t.Fatalf("non-advancing cursor %q", input.Cursor)
		}
		input.Cursor = result.NextCursor
	}
	t.Fatal("pagination did not complete")
}
