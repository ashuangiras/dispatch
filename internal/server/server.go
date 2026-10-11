// Package server exposes Dispatch's runs as MCP tools.
package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ashuangiras/dispatch/internal/capture"
	"github.com/ashuangiras/dispatch/internal/runs"
)

// Response bounds.
const (
	defaultWaitSeconds   = 30.0
	maxWaitSeconds       = 300.0
	defaultMaxLines      = 200
	maxMaxLines          = 2000
	defaultMaxBytes      = 32 << 10
	maxMaxBytes          = 256 << 10
	defaultMaxLineBytes  = 4 << 10
	maxTailLines         = 100
	tailLineBytes        = 1 << 10
	tailBudget           = 8 << 10
	defaultContextLines  = 2
	maxContextLines      = 20
	defaultMaxMatches    = 20
	maxMaxMatches        = 200
	searchLineBytes      = 1 << 20
	defaultListLimit     = 20
	maxListLimit         = 100
	maxDeleteIDs         = 100
	lineOverhead         = 32
	argsPreviewBytes     = 200
	defaultRunTailLines  = 10
	defaultStatusTailLen = 0
)

// Instructions is the server's guide for clients.
const Instructions = `Dispatch runs commands asynchronously and keeps their complete output so you can inspect it precisely without rerunning them.

Loop: run_command (wait=false to continue other work, or wait=true to block up to wait_timeout_seconds) returns a run_id and status with a short tail. Use wait_for_run or get_status later; neither returns the whole output. Read output with read_output (line ranges, negative start_line for the end, or the next_cursor from a previous read for incremental reads) and find things with search_output (literal or RE2 regex, with context lines). Every read is bounded: more_available says there is more, response_truncated says this response left something out, and output.lost_bytes / capture_error report output that was never stored. cancel_run stops a run and its process group. delete_runs removes finished runs; list_runs finds runs again.

Commands are executed directly with an exact argument array, never through a shell; to use shell syntax, run a shell explicitly (e.g. command "sh", args ["-c", "..."]).`

// Server holds the MCP tool handlers.
type Server struct {
	m *runs.Manager
}

// New builds an MCP server for m.
func New(m *runs.Manager, version string) *mcp.Server {
	s := &Server{m: m}
	srv := mcp.NewServer(&mcp.Implementation{Name: "dispatch", Version: version}, &mcp.ServerOptions{Instructions: Instructions})
	yes, no := true, false
	mcp.AddTool(srv, &mcp.Tool{Name: "run_command", Description: runCommandDesc,
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, OpenWorldHint: &yes}}, s.runCommand)
	mcp.AddTool(srv, &mcp.Tool{Name: "get_status", Description: getStatusDesc,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}}, s.getStatus)
	mcp.AddTool(srv, &mcp.Tool{Name: "wait_for_run", Description: waitDesc,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}}, s.waitForRun)
	mcp.AddTool(srv, &mcp.Tool{Name: "read_output", Description: readDesc,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}}, s.readOutput)
	mcp.AddTool(srv, &mcp.Tool{Name: "search_output", Description: searchDesc,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}}, s.searchOutput)
	mcp.AddTool(srv, &mcp.Tool{Name: "cancel_run", Description: cancelDesc,
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, IdempotentHint: true, OpenWorldHint: &no}}, s.cancelRun)
	mcp.AddTool(srv, &mcp.Tool{Name: "list_runs", Description: listDesc,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}}, s.listRuns)
	mcp.AddTool(srv, &mcp.Tool{Name: "delete_runs", Description: deleteDesc,
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, IdempotentHint: true, OpenWorldHint: &no}}, s.deleteRuns)
	return srv
}

const runCommandDesc = `Start a command. command is an executable name (looked up in the child's PATH) or a path containing '/' (relative paths resolve against cwd); args are passed exactly, with no shell interpretation. stdin is empty (/dev/null).

Environment: the child starts from Dispatch's own environment (env.inherit true, the default) or an empty one (inherit false); env.unset keys are removed, then env.set is applied. A key may not be in both. Changes affect only this child. Responses report variable names, never values.

wait (required): false returns at once with the run_id; true waits until the run finishes or wait_timeout_seconds (default 30, max 300) passes, after which the command keeps running and you can call wait_for_run. timeout_seconds is separate: when it expires Dispatch terminates the command's process group (SIGTERM, then SIGKILL after kill_grace_seconds). A command that cannot start (not found, bad cwd) returns a run in state failed_to_start with error set, not a tool error.

The response is the run status plus the last tail_lines (default 10) combined output lines; it never contains the whole output.`

const getStatusDesc = `Get a run's state: running, exited (exit_code or signal, and termination: exit, signal, timeout, cancelled or server_shutdown), failed_to_start (error) or interrupted (the Dispatch instance that owned it stopped first; outcome unknown). Includes timestamps and output counts per stream, lost bytes and capture errors. tail_lines (default 0, max 100) adds the last combined lines.`

const waitDesc = `Wait for a run to finish, up to timeout_seconds (default 30, max 300). If the deadline passes first, wait.deadline_reached is true and the command keeps running; call again to keep waiting. Cancelling this request ends the wait, never the run. Returns the status and the last tail_lines (default 10) combined lines.`

const readDesc = `Read a run's retained output by line, while it runs or after.

stream: stdout, stderr, or combined (default). Each stream keeps its exact byte order. combined interleaves complete lines in the order Dispatch received them; stdout and stderr are separate pipes, so this order across streams is approximate. An in-progress last line (no newline yet) appears with partial: true in stdout/stderr views only.

Position: start_line (1-based; negative counts from the end, -50 = last 50 lines), or cursor (the next_cursor of a previous read, for incremental reads that never skip or repeat a complete line). Bounds: max_lines (default 200, max 2000), max_bytes (default 32768, max 262144), max_line_bytes (default 4096): longer lines come back with truncated: true and byte_offset.

Bytes: set byte_offset (with stream stdout or stderr) to read raw bytes from that offset, up to max_bytes; encoding text (UTF-8, invalid bytes replaced) or base64 (exact bytes).

more_available: more output exists past this response (continue with next_cursor). response_truncated: this response left something out because of its bounds. Neither means output was lost; output.lost_bytes and output.capture_error report output that was never stored.`

const searchDesc = `Search a run's retained output for pattern: a literal string, or an RE2 regular expression when regex is true; ignore_case for case-insensitive matching. Searches complete lines of stream (default combined) from start_line or cursor, returning up to max_matches (default 20, max 200) matches, each with context_lines (default 2, max 20) lines around it; overlapping contexts are merged into one group.

scan_complete true means every line up to scanned_through was searched; otherwise continue with next_cursor. next_cursor also resumes later on a running command to search only new lines. Only the first 1 MiB of a longer line is searched (long_lines_partially_searched counts them). Bounded by max_bytes like read_output.`

const cancelDesc = `Cancel a running command: Dispatch sends SIGTERM to its process group (the command and every descendant that stayed in its group), then SIGKILL after grace_seconds (default: the run's kill_grace_seconds) if the command has not exited, and waits briefly for the run to finish. Returns the status (termination cancelled). Cancelling a finished run changes nothing. Descendants that moved to their own process group or session are not tracked.`

const listDesc = `List retained runs, newest first, with a short summary each. Optional state filter (running, exited, failed_to_start, interrupted). Paged: limit (default 20, max 100) and cursor (the next_cursor of the previous page).`

const deleteDesc = `Delete retained runs and their output. Give run_ids (up to 100), or all_finished true (optionally only those that ended more than older_than_seconds ago). Running runs are never deleted; they are reported in skipped. Dispatch also removes the oldest finished runs automatically beyond its retention limits (age, count and total bytes).`

// RunCommandInput is run_command's input.
type RunCommandInput struct {
	Command            string    `json:"command" jsonschema:"executable name (searched in the child's PATH) or a path containing '/'"`
	Args               []string  `json:"args,omitempty" jsonschema:"arguments, passed exactly as given"`
	Cwd                string    `json:"cwd,omitempty" jsonschema:"working directory; default: Dispatch's working directory; relative paths resolve against it"`
	Env                *EnvInput `json:"env,omitempty" jsonschema:"child environment changes"`
	Wait               bool      `json:"wait" jsonschema:"true: wait for completion up to wait_timeout_seconds; false: return at once"`
	WaitTimeoutSeconds *float64  `json:"wait_timeout_seconds,omitempty" jsonschema:"bounded wait when wait is true (default 30, max 300)"`
	TimeoutSeconds     *float64  `json:"timeout_seconds,omitempty" jsonschema:"execution timeout: terminate the command after this many seconds (default: none)"`
	KillGraceSeconds   *float64  `json:"kill_grace_seconds,omitempty" jsonschema:"seconds between SIGTERM and SIGKILL on timeout or cancel (default 5)"`
	MaxOutputBytes     *int64    `json:"max_output_bytes,omitempty" jsonschema:"bytes to store for both streams together (default and maximum: the server's limit); later output is counted as lost"`
	TailLines          *int      `json:"tail_lines,omitempty" jsonschema:"last combined lines to include (default 10, max 100)"`
	Label              string    `json:"label,omitempty" jsonschema:"free-text name shown in list_runs"`
}

// EnvInput is the child environment specification.
type EnvInput struct {
	Inherit *bool             `json:"inherit,omitempty" jsonschema:"start from Dispatch's environment (default true) or from an empty one"`
	Set     map[string]string `json:"set,omitempty" jsonschema:"variables to set or override"`
	Unset   []string          `json:"unset,omitempty" jsonschema:"variables to remove"`
}

func seconds(p *float64, def, max float64, name string) (time.Duration, error) {
	v := def
	if p != nil {
		v = *p
	}
	if v < 0 || (max > 0 && v > max) {
		return 0, fmt.Errorf("%s must be between 0 and %g", name, max)
	}
	return time.Duration(v * float64(time.Second)), nil
}

func intIn(p *int, def, lo, hi int, name string) (int, error) {
	v := def
	if p != nil {
		v = *p
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("%s must be between %d and %d", name, lo, hi)
	}
	return v, nil
}

func (s *Server) runCommand(ctx context.Context, _ *mcp.CallToolRequest, in RunCommandInput) (*mcp.CallToolResult, RunStatus, error) {
	waitFor, err := seconds(in.WaitTimeoutSeconds, defaultWaitSeconds, maxWaitSeconds, "wait_timeout_seconds")
	if err != nil {
		return nil, RunStatus{}, err
	}
	timeout, err := seconds(in.TimeoutSeconds, 0, 0, "timeout_seconds")
	if err != nil {
		return nil, RunStatus{}, err
	}
	grace, err := seconds(in.KillGraceSeconds, 0, 0, "kill_grace_seconds")
	if err != nil {
		return nil, RunStatus{}, err
	}
	tail, err := intIn(in.TailLines, defaultRunTailLines, 0, maxTailLines, "tail_lines")
	if err != nil {
		return nil, RunStatus{}, err
	}
	req := runs.StartRequest{Command: in.Command, Args: in.Args, Cwd: in.Cwd, Timeout: timeout, KillGrace: grace, Label: in.Label}
	if in.MaxOutputBytes != nil {
		if *in.MaxOutputBytes <= 0 {
			return nil, RunStatus{}, errors.New("max_output_bytes must be positive")
		}
		req.MaxOutputBytes = *in.MaxOutputBytes
	}
	if in.Env != nil {
		req.Env = runs.EnvSpec{Inherit: in.Env.Inherit, Set: in.Env.Set, Unset: in.Env.Unset}
	}
	snap, err := s.m.Start(req)
	if err != nil {
		return nil, RunStatus{}, err
	}
	var wi *WaitInfo
	if in.Wait {
		start := time.Now()
		var fin bool
		snap, fin, err = s.m.Wait(ctx, snap.ID, waitFor)
		if err != nil {
			return nil, RunStatus{}, err
		}
		wi = &WaitInfo{Finished: fin, DeadlineReached: !fin && ctx.Err() == nil, WaitedSeconds: time.Since(start).Seconds()}
	}
	return nil, s.status(snap, wi, tail), nil
}

func (s *Server) status(snap runs.Snapshot, wi *WaitInfo, tail int) RunStatus {
	st := runStatus(snap)
	st.Wait = wi
	if tail > 0 {
		st.Tail = readTail(snap, tail)
	}
	return st
}

func readTail(snap runs.Snapshot, n int) *Tail {
	t := &Tail{Lines: []OutLine{}}
	rd, err := capture.OpenReader(snap.Dir, snap.Capture.Active)
	if err != nil {
		return t
	}
	defer rd.Close()
	c, err := rd.Counts()
	if err != nil {
		return t
	}
	from := max(c.Combined-int64(n)+1, 1)
	budget := tailBudget
	var lines []OutLine
	for k := c.Combined; k >= from; k-- {
		l, err := rd.Line(capture.ViewCombined, k, tailLineBytes)
		if err != nil {
			break
		}
		ol := outLine(l, capture.ViewCombined)
		cost := len(ol.Text) + lineOverhead
		if cost > budget {
			t.Truncated = true
			break
		}
		budget -= cost
		lines = append(lines, ol)
	}
	for i := len(lines) - 1; i >= 0; i-- {
		t.Lines = append(t.Lines, lines[i])
	}
	return t
}

func outLine(l capture.Line, v capture.View) OutLine {
	ol := OutLine{N: l.N, Partial: l.Partial, Unterminated: !l.Partial && !l.Terminated}
	if v == capture.ViewCombined {
		ol.Stream = l.Stream.String()
	}
	if utf8.Valid(l.Data) {
		ol.Text = string(l.Data)
	} else {
		ol.Text = strings.ToValidUTF8(string(l.Data), "\uFFFD")
		ol.InvalidUTF8 = true
	}
	if l.Truncated() {
		ol.Truncated = true
		ol.Bytes = l.Length
		off := l.Offset
		ol.ByteOffset = &off
	}
	return ol
}

// RunIDInput selects a run.
type RunIDInput struct {
	RunID     string `json:"run_id"`
	TailLines *int   `json:"tail_lines,omitempty" jsonschema:"last combined lines to include (default 0, max 100)"`
}

func (s *Server) getStatus(_ context.Context, _ *mcp.CallToolRequest, in RunIDInput) (*mcp.CallToolResult, RunStatus, error) {
	tail, err := intIn(in.TailLines, defaultStatusTailLen, 0, maxTailLines, "tail_lines")
	if err != nil {
		return nil, RunStatus{}, err
	}
	snap, err := s.m.Get(in.RunID)
	if err != nil {
		return nil, RunStatus{}, err
	}
	return nil, s.status(snap, nil, tail), nil
}

// WaitInput is wait_for_run's input.
type WaitInput struct {
	RunID          string   `json:"run_id"`
	TimeoutSeconds *float64 `json:"timeout_seconds,omitempty" jsonschema:"wait deadline in seconds (default 30, max 300); the command keeps running after it"`
	TailLines      *int     `json:"tail_lines,omitempty" jsonschema:"last combined lines to include (default 10, max 100)"`
}

func (s *Server) waitForRun(ctx context.Context, _ *mcp.CallToolRequest, in WaitInput) (*mcp.CallToolResult, RunStatus, error) {
	d, err := seconds(in.TimeoutSeconds, defaultWaitSeconds, maxWaitSeconds, "timeout_seconds")
	if err != nil {
		return nil, RunStatus{}, err
	}
	tail, err := intIn(in.TailLines, defaultRunTailLines, 0, maxTailLines, "tail_lines")
	if err != nil {
		return nil, RunStatus{}, err
	}
	start := time.Now()
	snap, fin, err := s.m.Wait(ctx, in.RunID, d)
	if err != nil {
		return nil, RunStatus{}, err
	}
	wi := &WaitInfo{Finished: fin, DeadlineReached: !fin && ctx.Err() == nil, WaitedSeconds: time.Since(start).Seconds()}
	return nil, s.status(snap, wi, tail), nil
}

// CaptureSummary repeats, in read and search results, whether output was lost.
type CaptureSummary struct {
	Active       bool   `json:"active" jsonschema:"output may still grow"`
	Complete     bool   `json:"complete" jsonschema:"capture finished and nothing was lost"`
	LostBytes    int64  `json:"lost_bytes" jsonschema:"bytes written by the command but never stored"`
	LimitReached bool   `json:"limit_reached,omitempty"`
	Error        string `json:"error,omitempty"`
}

func captureSummary(c capture.Stats) CaptureSummary {
	return CaptureSummary{Active: c.Active, Complete: !c.Active && c.Lost() == 0 && c.Failure == "",
		LostBytes: c.Lost(), LimitReached: c.LimitReached, Error: c.Failure}
}

// ReadInput is read_output's input.
type ReadInput struct {
	RunID        string `json:"run_id"`
	Stream       string `json:"stream,omitempty" jsonschema:"stdout, stderr or combined (default)"`
	StartLine    *int64 `json:"start_line,omitempty" jsonschema:"first line, 1-based; negative counts from the end (-1 is the last line); default 1"`
	Cursor       string `json:"cursor,omitempty" jsonschema:"next_cursor from a previous read_output or search_output on the same stream; overrides start_line"`
	MaxLines     *int   `json:"max_lines,omitempty" jsonschema:"default 200, max 2000"`
	MaxBytes     *int   `json:"max_bytes,omitempty" jsonschema:"text bytes in the response, default 32768, max 262144"`
	MaxLineBytes *int   `json:"max_line_bytes,omitempty" jsonschema:"bytes of each line, default 4096, at most max_bytes"`
	ByteOffset   *int64 `json:"byte_offset,omitempty" jsonschema:"byte mode: read raw bytes of stdout or stderr from this offset"`
	Encoding     string `json:"encoding,omitempty" jsonschema:"byte mode: text (default) or base64"`
}

// ReadResult is read_output's result.
type ReadResult struct {
	RunID             string         `json:"run_id"`
	Stream            string         `json:"stream"`
	State             string         `json:"state"`
	Lines             []OutLine      `json:"lines,omitempty"`
	FirstLine         int64          `json:"first_line,omitempty"`
	LastLine          int64          `json:"last_line,omitempty"`
	TotalLines        int64          `json:"total_lines" jsonschema:"complete lines currently in this view"`
	NextCursor        string         `json:"next_cursor,omitempty" jsonschema:"pass as cursor to continue after this response"`
	MoreAvailable     bool           `json:"more_available" jsonschema:"output exists beyond this response now"`
	ResponseTruncated bool           `json:"response_truncated" jsonschema:"this response omitted output because of max_lines, max_bytes or max_line_bytes"`
	Data              *string        `json:"data,omitempty" jsonschema:"byte mode: the bytes read"`
	Encoding          string         `json:"encoding,omitempty"`
	ByteOffset        *int64         `json:"byte_offset,omitempty"`
	NextByteOffset    *int64         `json:"next_byte_offset,omitempty"`
	TotalBytes        *int64         `json:"total_bytes,omitempty" jsonschema:"byte mode: bytes stored for the stream"`
	InvalidUTF8       bool           `json:"invalid_utf8,omitempty"`
	Capture           CaptureSummary `json:"capture"`
}

func parseCursor(cursor string, v capture.View, explicit bool) (capture.View, int64, error) {
	name, num, ok := strings.Cut(cursor, ":")
	n, err := strconv.ParseInt(num, 10, 64)
	if !ok || err != nil || n < 1 {
		return 0, 0, fmt.Errorf("invalid cursor %q", cursor)
	}
	cv, err := capture.ParseView(name)
	if err != nil || name == "" {
		return 0, 0, fmt.Errorf("invalid cursor %q", cursor)
	}
	if explicit && cv != v {
		return 0, 0, fmt.Errorf("cursor %q is for stream %s, not %s", cursor, cv, v)
	}
	return cv, n, nil
}

func cursorFor(v capture.View, n int64) string { return fmt.Sprintf("%s:%d", v, n) }

func (s *Server) readOutput(_ context.Context, _ *mcp.CallToolRequest, in ReadInput) (*mcp.CallToolResult, ReadResult, error) {
	view, err := capture.ParseView(in.Stream)
	if err != nil {
		return nil, ReadResult{}, err
	}
	maxBytes, err := intIn(in.MaxBytes, defaultMaxBytes, 1, maxMaxBytes, "max_bytes")
	if err != nil {
		return nil, ReadResult{}, err
	}
	snap, err := s.m.Get(in.RunID)
	if err != nil {
		return nil, ReadResult{}, err
	}
	rd, err := capture.OpenReader(snap.Dir, snap.Capture.Active)
	if err != nil {
		return nil, ReadResult{}, fmt.Errorf("opening output: %w", err)
	}
	defer rd.Close()
	res := ReadResult{RunID: snap.ID, State: string(snap.State), Capture: captureSummary(snap.Capture)}

	if in.ByteOffset != nil {
		if view == capture.ViewCombined {
			return nil, ReadResult{}, errors.New("byte reads need stream stdout or stderr")
		}
		return nil, readBytes(rd, res, capture.Stream(view), *in.ByteOffset, maxBytes, in.Encoding), nil
	}
	if in.Encoding != "" && in.Encoding != "text" {
		return nil, ReadResult{}, errors.New("encoding applies to byte reads (byte_offset); line reads are text")
	}
	maxLines, err := intIn(in.MaxLines, defaultMaxLines, 1, maxMaxLines, "max_lines")
	if err != nil {
		return nil, ReadResult{}, err
	}
	maxLine, err := intIn(in.MaxLineBytes, min(defaultMaxLineBytes, maxBytes), 1, maxBytes, "max_line_bytes")
	if err != nil {
		return nil, ReadResult{}, err
	}
	c, err := rd.Counts()
	if err != nil {
		return nil, ReadResult{}, err
	}
	total := c.ViewLines(view)
	start := int64(1)
	if in.Cursor != "" {
		if view, start, err = parseCursor(in.Cursor, view, in.Stream != ""); err != nil {
			return nil, ReadResult{}, err
		}
		total = c.ViewLines(view)
	} else if in.StartLine != nil {
		start = *in.StartLine
		if start < 0 {
			start = max(total+start+1, 1)
		} else if start == 0 {
			start = 1
		}
	}
	res.Stream = view.String()
	res.TotalLines = total
	res.Lines = []OutLine{}
	budget := maxBytes
	n := start
	for ; n <= total && len(res.Lines) < maxLines; n++ {
		l, err := rd.Line(view, n, maxLine)
		if err != nil {
			return nil, ReadResult{}, err
		}
		ol := outLine(l, view)
		cost := len(ol.Text) + lineOverhead
		if len(res.Lines) > 0 && cost > budget {
			res.ResponseTruncated = true
			break
		}
		budget -= cost
		if ol.Truncated {
			res.ResponseTruncated = true
		}
		res.Lines = append(res.Lines, ol)
	}
	if n <= total {
		res.MoreAvailable = true
		if len(res.Lines) == maxLines {
			res.ResponseTruncated = true
		}
	}
	if n > total && view != capture.ViewCombined {
		if pl, ok, err := rd.PartialLine(capture.Stream(view), c, maxLine); err == nil && ok {
			ol := outLine(pl, view)
			if len(res.Lines) < maxLines && len(ol.Text)+lineOverhead <= budget {
				res.Lines = append(res.Lines, ol)
			} else {
				res.MoreAvailable = true
			}
		}
	}
	if len(res.Lines) > 0 {
		res.FirstLine = res.Lines[0].N
		res.LastLine = res.Lines[len(res.Lines)-1].N
	}
	res.NextCursor = cursorFor(view, n)
	return nil, res, nil
}

func readBytes(rd *capture.Reader, res ReadResult, s capture.Stream, off int64, maxBytes int, enc string) ReadResult {
	res.Stream = s.String()
	if off < 0 {
		off = 0
	}
	data, size, err := rd.ReadBytes(s, off, maxBytes)
	if err != nil {
		data = nil
	}
	switch enc {
	case "base64":
		str := base64.StdEncoding.EncodeToString(data)
		res.Data = &str
	default:
		enc = "text"
		if off+int64(len(data)) < size {
			// Do not split a UTF-8 sequence at the end of a chunk.
			for i := 1; i <= 3 && i <= len(data); i++ {
				if utf8.RuneStart(data[len(data)-i]) {
					if !utf8.FullRune(data[len(data)-i:]) {
						data = data[:len(data)-i]
					}
					break
				}
			}
		}
		str := string(data)
		if !utf8.Valid(data) {
			str = strings.ToValidUTF8(str, "\uFFFD")
			res.InvalidUTF8 = true
		}
		res.Data = &str
	}
	next := off + int64(len(data))
	res.Encoding = enc
	res.ByteOffset = &off
	res.NextByteOffset = &next
	res.TotalBytes = &size
	res.MoreAvailable = next < size
	res.ResponseTruncated = next < size
	return res
}

// SearchInput is search_output's input.
type SearchInput struct {
	RunID        string `json:"run_id"`
	Pattern      string `json:"pattern" jsonschema:"text to find; an RE2 regular expression when regex is true"`
	Regex        bool   `json:"regex,omitempty"`
	IgnoreCase   bool   `json:"ignore_case,omitempty"`
	Stream       string `json:"stream,omitempty" jsonschema:"stdout, stderr or combined (default)"`
	ContextLines *int   `json:"context_lines,omitempty" jsonschema:"lines before and after each match, default 2, max 20"`
	MaxMatches   *int   `json:"max_matches,omitempty" jsonschema:"default 20, max 200"`
	StartLine    *int64 `json:"start_line,omitempty" jsonschema:"first line to search, 1-based; negative counts from the end"`
	Cursor       string `json:"cursor,omitempty" jsonschema:"next_cursor of a previous search or read on the same stream"`
	MaxBytes     *int   `json:"max_bytes,omitempty" jsonschema:"text bytes in the response, default 32768, max 262144"`
	MaxLineBytes *int   `json:"max_line_bytes,omitempty" jsonschema:"bytes of each returned line, default 4096"`
}

// MatchGroup is a run of consecutive lines holding one or more matches.
type MatchGroup struct {
	Lines []OutLine `json:"lines"`
}

// SearchResult is search_output's result.
type SearchResult struct {
	RunID                      string         `json:"run_id"`
	Stream                     string         `json:"stream"`
	State                      string         `json:"state"`
	Groups                     []MatchGroup   `json:"groups"`
	Matches                    int            `json:"matches" jsonschema:"matching lines returned"`
	ScannedFrom                int64          `json:"scanned_from"`
	ScannedThrough             int64          `json:"scanned_through" jsonschema:"last line searched"`
	TotalLines                 int64          `json:"total_lines"`
	ScanComplete               bool           `json:"scan_complete" jsonschema:"every currently stored complete line from scanned_from on was searched"`
	NextCursor                 string         `json:"next_cursor" jsonschema:"continue the search after the last returned match, or after the end"`
	MoreAvailable              bool           `json:"more_available"`
	ResponseTruncated          bool           `json:"response_truncated" jsonschema:"matches or context were left out because of max_bytes or max_line_bytes"`
	LongLinesPartiallySearched int            `json:"long_lines_partially_searched,omitempty"`
	Capture                    CaptureSummary `json:"capture"`
}

func (s *Server) searchOutput(ctx context.Context, _ *mcp.CallToolRequest, in SearchInput) (*mcp.CallToolResult, SearchResult, error) {
	view, err := capture.ParseView(in.Stream)
	if err != nil {
		return nil, SearchResult{}, err
	}
	if in.Pattern == "" {
		return nil, SearchResult{}, errors.New("pattern is required")
	}
	expr := in.Pattern
	if !in.Regex {
		expr = regexp.QuoteMeta(expr)
	}
	if in.IgnoreCase {
		expr = "(?i)" + expr
	}
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, SearchResult{}, fmt.Errorf("invalid regex: %w", err)
	}
	ctxLines, err := intIn(in.ContextLines, defaultContextLines, 0, maxContextLines, "context_lines")
	if err != nil {
		return nil, SearchResult{}, err
	}
	maxMatches, err := intIn(in.MaxMatches, defaultMaxMatches, 1, maxMaxMatches, "max_matches")
	if err != nil {
		return nil, SearchResult{}, err
	}
	maxBytes, err := intIn(in.MaxBytes, defaultMaxBytes, 1, maxMaxBytes, "max_bytes")
	if err != nil {
		return nil, SearchResult{}, err
	}
	maxLine, err := intIn(in.MaxLineBytes, min(defaultMaxLineBytes, maxBytes), 1, maxBytes, "max_line_bytes")
	if err != nil {
		return nil, SearchResult{}, err
	}
	snap, err := s.m.Get(in.RunID)
	if err != nil {
		return nil, SearchResult{}, err
	}
	rd, err := capture.OpenReader(snap.Dir, snap.Capture.Active)
	if err != nil {
		return nil, SearchResult{}, fmt.Errorf("opening output: %w", err)
	}
	defer rd.Close()
	c, err := rd.Counts()
	if err != nil {
		return nil, SearchResult{}, err
	}
	total := c.ViewLines(view)
	start := int64(1)
	if in.Cursor != "" {
		if view, start, err = parseCursor(in.Cursor, view, in.Stream != ""); err != nil {
			return nil, SearchResult{}, err
		}
		total = c.ViewLines(view)
	} else if in.StartLine != nil {
		start = *in.StartLine
		if start < 0 {
			start = max(total+start+1, 1)
		} else if start == 0 {
			start = 1
		}
	}
	res := SearchResult{RunID: snap.ID, Stream: view.String(), State: string(snap.State), Groups: []MatchGroup{},
		ScannedFrom: start, TotalLines: total, Capture: captureSummary(snap.Capture)}
	var matches []int64
	scanned := start - 1
	err = rd.Scan(ctx, view, start, total, searchLineBytes, func(l capture.Line) bool {
		scanned = l.N
		if l.Truncated() {
			res.LongLinesPartiallySearched++
		}
		if re.Match(l.Data) {
			matches = append(matches, l.N)
		}
		return len(matches) < maxMatches
	})
	if err != nil {
		return nil, SearchResult{}, err
	}
	res.ScannedThrough = max(scanned, 0)
	res.ScanComplete = scanned >= total
	next := scanned + 1

	isMatch := map[int64]bool{}
	for _, m := range matches {
		isMatch[m] = true
	}
	budget := maxBytes
	var last int64
	for i := 0; i < len(matches); {
		lo := max(matches[i]-int64(ctxLines), last+1, 1)
		hi := min(matches[i]+int64(ctxLines), total)
		j := i + 1
		for j < len(matches) && matches[j]-int64(ctxLines) <= hi+1 {
			hi = min(matches[j]+int64(ctxLines), total)
			j++
		}
		var g MatchGroup
		var cost int
		for n := lo; n <= hi; n++ {
			l, err := rd.Line(view, n, maxLine)
			if err != nil {
				return nil, SearchResult{}, err
			}
			ol := outLine(l, view)
			ol.Match = isMatch[n]
			if ol.Truncated {
				res.ResponseTruncated = true
			}
			cost += len(ol.Text) + lineOverhead
			g.Lines = append(g.Lines, ol)
		}
		if len(res.Groups) > 0 && cost > budget {
			res.ResponseTruncated = true
			res.ScanComplete = false
			next = matches[i]
			break
		}
		budget -= cost
		res.Groups = append(res.Groups, g)
		res.Matches += j - i
		last = hi
		i = j
	}
	res.NextCursor = cursorFor(view, next)
	res.MoreAvailable = !res.ScanComplete
	return nil, res, nil
}

// CancelInput is cancel_run's input.
type CancelInput struct {
	RunID        string   `json:"run_id"`
	GraceSeconds *float64 `json:"grace_seconds,omitempty" jsonschema:"seconds between SIGTERM and SIGKILL (default: the run's kill_grace_seconds)"`
}

func (s *Server) cancelRun(_ context.Context, _ *mcp.CallToolRequest, in CancelInput) (*mcp.CallToolResult, RunStatus, error) {
	grace, err := seconds(in.GraceSeconds, 0, 0, "grace_seconds")
	if err != nil {
		return nil, RunStatus{}, err
	}
	snap, err := s.m.Cancel(in.RunID, grace)
	if err != nil {
		return nil, RunStatus{}, err
	}
	return nil, s.status(snap, nil, 0), nil
}

// ListInput is list_runs's input.
type ListInput struct {
	State  string `json:"state,omitempty" jsonschema:"only runs in this state: running, exited, failed_to_start or interrupted"`
	Limit  *int   `json:"limit,omitempty" jsonschema:"default 20, max 100"`
	Cursor string `json:"cursor,omitempty" jsonschema:"next_cursor of the previous page"`
}

// RunSummary is one run in list_runs.
type RunSummary struct {
	RunID       string `json:"run_id"`
	State       string `json:"state"`
	Label       string `json:"label,omitempty"`
	CommandLine string `json:"command_line" jsonschema:"command and args joined by spaces, cut to 200 bytes; for display only"`
	CreatedAt   string `json:"created_at"`
	EndedAt     string `json:"ended_at,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	Signal      string `json:"signal,omitempty"`
	Termination string `json:"termination,omitempty"`
	StoredBytes int64  `json:"stored_bytes"`
	LostBytes   int64  `json:"lost_bytes,omitempty"`
}

// ListResult is list_runs's result.
type ListResult struct {
	Runs       []RunSummary `json:"runs"`
	Total      int          `json:"total" jsonschema:"runs matching the filter"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

func (s *Server) listRuns(_ context.Context, _ *mcp.CallToolRequest, in ListInput) (*mcp.CallToolResult, ListResult, error) {
	limit, err := intIn(in.Limit, defaultListLimit, 1, maxListLimit, "limit")
	if err != nil {
		return nil, ListResult{}, err
	}
	offset := 0
	if in.Cursor != "" {
		if offset, err = strconv.Atoi(in.Cursor); err != nil || offset < 0 {
			return nil, ListResult{}, fmt.Errorf("invalid cursor %q", in.Cursor)
		}
	}
	switch runs.State(in.State) {
	case "", runs.StateRunning, runs.StateExited, runs.StateFailedToStart, runs.StateInterrupted:
	default:
		return nil, ListResult{}, fmt.Errorf("unknown state %q", in.State)
	}
	all, err := s.m.List(runs.State(in.State))
	if err != nil {
		return nil, ListResult{}, err
	}
	res := ListResult{Runs: []RunSummary{}, Total: len(all)}
	for i := offset; i < len(all) && len(res.Runs) < limit; i++ {
		r := all[i]
		cl := strings.Join(append([]string{r.Command}, r.Args...), " ")
		if len(cl) > argsPreviewBytes {
			cl = strings.ToValidUTF8(cl[:argsPreviewBytes], "") + "…"
		}
		res.Runs = append(res.Runs, RunSummary{
			RunID: r.ID, State: string(r.State), Label: r.Label, CommandLine: cl,
			CreatedAt: ts(&r.CreatedAt), EndedAt: ts(r.EndedAt), ExitCode: r.ExitCode, Signal: r.Signal,
			Termination: r.Termination, StoredBytes: r.Capture.StoredBytes, LostBytes: r.Capture.Lost(),
		})
	}
	if end := offset + len(res.Runs); end < len(all) {
		res.NextCursor = strconv.Itoa(end)
	}
	return nil, res, nil
}

// DeleteInput is delete_runs's input.
type DeleteInput struct {
	RunIDs           []string `json:"run_ids,omitempty" jsonschema:"runs to delete (up to 100)"`
	AllFinished      bool     `json:"all_finished,omitempty" jsonschema:"delete every finished run"`
	OlderThanSeconds *float64 `json:"older_than_seconds,omitempty" jsonschema:"with all_finished: only runs that ended more than this long ago"`
}

// Skipped is a run delete_runs did not delete.
type Skipped struct {
	RunID  string `json:"run_id"`
	Reason string `json:"reason"`
}

// DeleteResult is delete_runs's result.
type DeleteResult struct {
	Deleted    []string  `json:"deleted"`
	Skipped    []Skipped `json:"skipped"`
	FreedBytes int64     `json:"freed_bytes"`
}

func (s *Server) deleteRuns(_ context.Context, _ *mcp.CallToolRequest, in DeleteInput) (*mcp.CallToolResult, DeleteResult, error) {
	if (len(in.RunIDs) > 0) == in.AllFinished {
		return nil, DeleteResult{}, errors.New("give either run_ids or all_finished true")
	}
	if len(in.RunIDs) > maxDeleteIDs {
		return nil, DeleteResult{}, fmt.Errorf("at most %d run_ids per call", maxDeleteIDs)
	}
	if in.OlderThanSeconds != nil && !in.AllFinished {
		return nil, DeleteResult{}, errors.New("older_than_seconds applies only with all_finished")
	}
	ids := in.RunIDs
	if in.AllFinished {
		var cutoff time.Time
		if in.OlderThanSeconds != nil {
			if *in.OlderThanSeconds < 0 {
				return nil, DeleteResult{}, errors.New("older_than_seconds must not be negative")
			}
			cutoff = time.Now().Add(-time.Duration(*in.OlderThanSeconds * float64(time.Second)))
		}
		all, err := s.m.List("")
		if err != nil {
			return nil, DeleteResult{}, err
		}
		for _, r := range all {
			if r.Finished() && (cutoff.IsZero() || (r.EndedAt != nil && r.EndedAt.Before(cutoff)) || (r.EndedAt == nil && r.CreatedAt.Before(cutoff))) {
				ids = append(ids, r.ID)
			}
		}
	}
	res := DeleteResult{Deleted: []string{}, Skipped: []Skipped{}}
	for _, id := range ids {
		n, err := s.m.Delete(id)
		if err != nil {
			res.Skipped = append(res.Skipped, Skipped{RunID: id, Reason: err.Error()})
			continue
		}
		res.Deleted = append(res.Deleted, id)
		res.FreedBytes += n
	}
	return nil, res, nil
}
