package server

import (
	"context"
	"errors"
	"time"

	"github.com/ashuangiras/dispatch/internal/runs"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const waitOutputDesc = `Wait for the first complete retained output line matching one literal pattern or RE2 regex. Select stdout, stderr, or combined (default, approximate cross-stream order), and optionally resume from a read/search/wait cursor. Existing output is searched first; without a cursor, start at line 1. Partial lines are not searched until completed or the stream closes.

Returns outcome: match_found (an observed match, not proof of readiness or health), deadline_reached, run_ended (no match in fully searched retained output), capture_incomplete (loss/error or unknown capture statistics prevents trusting absence), or search_incomplete (a long line was only partially searched). search carries a bounded matching-line excerpt, capture status, line number and next_cursor. Use read_output for surrounding context or exact bytes. max_bytes bounds excerpt text (default 32768, max 262144); max_line_bytes defaults to 4096. A truncated excerpt does not mean lost capture.

timeout_seconds defaults to 30, max 300; zero checks current output once. The deadline and request cancellation end only this wait, never the command. Resume with next_cursor to avoid rescanning older output. Search coverage limits are the same as search_output.`

type WaitOutputInput struct {
	RunID          string   `json:"run_id"`
	Pattern        string   `json:"pattern"`
	Regex          bool     `json:"regex,omitempty"`
	IgnoreCase     bool     `json:"ignore_case,omitempty"`
	Stream         string   `json:"stream,omitempty" jsonschema:"stdout, stderr or combined (default)"`
	Cursor         string   `json:"cursor,omitempty" jsonschema:"resume cursor from read_output, search_output or wait_for_output on this stream"`
	TimeoutSeconds *float64 `json:"timeout_seconds,omitempty" jsonschema:"default 30, max 300; zero checks current output once without waiting for new output"`
	MaxBytes       *int     `json:"max_bytes,omitempty" jsonschema:"matching excerpt text budget, default 32768, max 262144"`
	MaxLineBytes   *int     `json:"max_line_bytes,omitempty" jsonschema:"matching-line preview bytes, default 4096, at most max_bytes"`
}

type WaitOutputResult struct {
	RunID         string        `json:"run_id"`
	Outcome       string        `json:"outcome" jsonschema:"match_found, deadline_reached, run_ended, capture_incomplete or search_incomplete"`
	WaitedSeconds float64       `json:"waited_seconds"`
	NextCursor    string        `json:"next_cursor,omitempty" jsonschema:"next unsearched line; on an interrupted scan, retain the original cursor"`
	Search        *SearchResult `json:"search,omitempty" jsonschema:"latest completed search, including bounded excerpt and capture status; absent if the first scan hit its deadline"`
}

func (s *Server) waitForOutput(ctx context.Context, _ *mcp.CallToolRequest, in WaitOutputInput) (*mcp.CallToolResult, WaitOutputResult, error) {
	d, err := seconds(in.TimeoutSeconds, defaultWaitSeconds, maxWaitSeconds, "timeout_seconds")
	if err != nil {
		return nil, WaitOutputResult{}, err
	}
	started := time.Now()
	scanCtx := ctx
	cancel := func() {}
	if d > 0 {
		scanCtx, cancel = context.WithTimeout(ctx, d)
	}
	defer cancel()
	one, zero := 1, 0
	query := SearchInput{RunID: in.RunID, Pattern: in.Pattern, Regex: in.Regex, IgnoreCase: in.IgnoreCase, Stream: in.Stream, Cursor: in.Cursor, MaxMatches: &one, ContextLines: &zero, MaxBytes: in.MaxBytes, MaxLineBytes: in.MaxLineBytes}
	result := WaitOutputResult{RunID: in.RunID, NextCursor: in.Cursor}
	finish := func(outcome string) (*mcp.CallToolResult, WaitOutputResult, error) {
		result.Outcome = outcome
		result.WaitedSeconds = time.Since(started).Seconds()
		return nil, result, nil
	}
	// Poll inside the server, not in the agent. Each successful scan resumes at
	// its next cursor, so retained output without a match is scanned only once.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, WaitOutputResult{}, err
		}
		_, found, err := s.searchOutput(scanCtx, nil, query)
		if err != nil {
			if ctx.Err() != nil {
				return nil, WaitOutputResult{}, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return finish("deadline_reached")
			}
			return nil, WaitOutputResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return nil, WaitOutputResult{}, err
		}
		if d > 0 && scanCtx.Err() != nil {
			// A scan finishing just after the deadline must not advance the
			// caller's cursor or claim a match observed within this wait.
			return finish("deadline_reached")
		}
		result.Search = &found
		result.NextCursor = found.NextCursor
		if found.Matches > 0 {
			return finish("match_found")
		}
		if found.Capture.StatsUnknown || found.Capture.Error != "" || (found.Capture.LostBytes != nil && *found.Capture.LostBytes > 0) {
			return finish("capture_incomplete")
		}
		if found.LongLinesPartiallySearched > 0 {
			return finish("search_incomplete")
		}
		if runs.State(found.State) != runs.StateRunning {
			return finish("run_ended")
		}
		query.Cursor = found.NextCursor
		if d == 0 {
			return finish("deadline_reached")
		}
		select {
		case <-ctx.Done():
			return nil, WaitOutputResult{}, ctx.Err()
		case <-scanCtx.Done():
			return finish("deadline_reached")
		case <-tick.C:
		}
	}
}
