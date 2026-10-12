package server

import (
	"time"

	"github.com/ashuangiras/dispatch/internal/capture"
	"github.com/ashuangiras/dispatch/internal/runs"
)

// OutLine is one line of output in a response.
type OutLine struct {
	N            int64  `json:"n" jsonschema:"line number in the selected view (1-based)"`
	Stream       string `json:"stream,omitempty" jsonschema:"stdout or stderr; set in the combined view"`
	Text         string `json:"text" jsonschema:"the line without its newline, decoded as UTF-8 (invalid bytes become U+FFFD)"`
	Match        bool   `json:"match,omitempty" jsonschema:"search results: this line matched"`
	Partial      bool   `json:"partial,omitempty" jsonschema:"the stream's last line, still being written (no newline yet); it may grow and is not counted in total_lines"`
	Unterminated bool   `json:"unterminated,omitempty" jsonschema:"the stream ended without a final newline on this line"`
	Truncated    bool   `json:"truncated,omitempty" jsonschema:"text holds only the first max_line_bytes of the line; read the rest with byte_offset"`
	Bytes        int64  `json:"bytes,omitempty" jsonschema:"full length of a truncated line in bytes"`
	ByteOffset   *int64 `json:"byte_offset,omitempty" jsonschema:"offset of a truncated line in its stream, for byte reads"`
	InvalidUTF8  bool   `json:"invalid_utf8,omitempty" jsonschema:"the line holds bytes that are not valid UTF-8; read it with encoding base64 for the exact bytes"`
}

// StreamStatus describes what was captured from one stream.
type StreamStatus struct {
	Bytes            int64  `json:"bytes" jsonschema:"bytes stored"`
	Lines            int64  `json:"lines" jsonschema:"complete lines stored"`
	LostBytes        *int64 `json:"lost_bytes" jsonschema:"bytes the command wrote that were not stored (limit or capture failure); null when output.stats_unknown is true (not necessarily zero)"`
	PartialLineBytes int64  `json:"partial_line_bytes,omitempty" jsonschema:"bytes of an in-progress last line"`
}

// OutputStatus describes the capture of a run.
type OutputStatus struct {
	CaptureActive bool         `json:"capture_active" jsonschema:"output may still grow"`
	Complete      bool         `json:"complete" jsonschema:"capture finished and every byte the command wrote is stored; false whenever stats_unknown is true"`
	Stdout        StreamStatus `json:"stdout"`
	Stderr        StreamStatus `json:"stderr"`
	CombinedLines int64        `json:"combined_lines" jsonschema:"complete lines in the combined view"`
	LimitBytes    int64        `json:"limit_bytes,omitempty" jsonschema:"bytes this run may store, both streams together"`
	LimitReached  *bool        `json:"limit_reached" jsonschema:"the storage limit was reached; later output is counted in lost_bytes, not stored; null when stats_unknown is true (not necessarily false)"`
	StatsUnknown  bool         `json:"stats_unknown,omitempty" jsonschema:"true when lost_bytes/limit_reached are not tracked by this Dispatch instance (the run is owned by another live instance); distinct from capture_error, an actual capture failure"`
	CaptureError  string       `json:"capture_error,omitempty" jsonschema:"why capture failed or stopped early; output after it is lost"`
}

// WaitInfo describes how a wait ended.
type WaitInfo struct {
	Finished        bool    `json:"finished" jsonschema:"the run is no longer running"`
	DeadlineReached bool    `json:"deadline_reached" jsonschema:"the wait deadline passed first; the command keeps running"`
	WaitedSeconds   float64 `json:"waited_seconds"`
}

// Tail is the last lines of the combined view.
type Tail struct {
	Lines     []OutLine `json:"lines"`
	Truncated bool      `json:"truncated,omitempty" jsonschema:"fewer lines than requested fit the tail's byte budget"`
}

// RunStatus is the state of one run.
type RunStatus struct {
	RunID           string         `json:"run_id"`
	State           string         `json:"state" jsonschema:"running, exited, failed_to_start or interrupted"`
	Label           string         `json:"label,omitempty"`
	LabelTruncated  bool           `json:"label_truncated,omitempty" jsonschema:"label was cut to a preview; call again with full_metadata for the whole value"`
	Command         string         `json:"command"`
	Executable      string         `json:"executable,omitempty" jsonschema:"the resolved path that was executed"`
	Args            []string       `json:"args"`
	ArgsTruncated   bool           `json:"args_truncated,omitempty" jsonschema:"args was cut to a preview, whole arguments kept where possible; call again with full_metadata for the exact, complete argv (the command itself always ran with the exact, untruncated args)"`
	Cwd             string         `json:"cwd"`
	Env             runs.EnvRecord `json:"env" jsonschema:"environment shape: values are never reported"`
	PID             int            `json:"pid,omitempty" jsonschema:"process ID and process-group ID of the command; informational only"`
	CreatedAt       string         `json:"created_at"`
	StartedAt       string         `json:"started_at,omitempty"`
	EndedAt         string         `json:"ended_at,omitempty"`
	DurationSeconds float64        `json:"duration_seconds,omitempty"`
	ExitCode        *int           `json:"exit_code,omitempty" jsonschema:"exit status when the command exited normally"`
	Signal          string         `json:"signal,omitempty" jsonschema:"signal that ended the command, e.g. SIGKILL"`
	Termination     string         `json:"termination,omitempty" jsonschema:"why the run ended: exit, signal, timeout, cancelled, server_shutdown or unknown"`
	Error           string         `json:"error,omitempty" jsonschema:"why the command failed to start, or why the run was interrupted"`
	TimeoutSeconds  float64        `json:"timeout_seconds,omitempty"`
	ProcessGroup    string         `json:"process_group,omitempty" jsonschema:"what Dispatch did to leftover processes in the run's process group"`
	OwnedElsewhere  bool           `json:"owned_elsewhere,omitempty" jsonschema:"another live Dispatch instance runs this command; this one can read but not cancel it"`
	Output          OutputStatus   `json:"output"`
	Wait            *WaitInfo      `json:"wait,omitempty"`
	Tail            *Tail          `json:"tail,omitempty"`
}

func ts(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func outputStatus(s runs.Snapshot) OutputStatus {
	c := s.Capture
	o := OutputStatus{
		CaptureActive: c.Active,
		Stdout:        StreamStatus{Bytes: c.Bytes[0], Lines: c.Lines[0]},
		Stderr:        StreamStatus{Bytes: c.Bytes[1], Lines: c.Lines[1]},
		LimitBytes:    c.MaxBytes,
		StatsUnknown:  c.StatsUnknown,
		CaptureError:  c.Failure,
	}
	if c.StatsUnknown {
		o.Complete = false
	} else {
		lost0, lost1 := c.LostBytes[0], c.LostBytes[1]
		o.Stdout.LostBytes, o.Stderr.LostBytes = &lost0, &lost1
		limitReached := c.LimitReached
		o.LimitReached = &limitReached
		o.Complete = !c.Active && c.Lost() == 0 && c.Failure == ""
	}
	if rd, err := capture.OpenReader(s.Dir, c.Active); err == nil {
		if counts, err := rd.Counts(); err == nil {
			o.CombinedLines = counts.Combined
			if c.Active {
				o.Stdout.PartialLineBytes = counts.PartialBytes[0]
				o.Stderr.PartialLineBytes = counts.PartialBytes[1]
				o.Stdout.Lines, o.Stderr.Lines = counts.Lines[0], counts.Lines[1]
			}
		}
		rd.Close()
	}
	return o
}

func runStatus(s runs.Snapshot, fullMetadata bool) RunStatus {
	label, labelTruncated := s.Label, false
	args, argsTruncated := s.Args, false
	if !fullMetadata {
		label, labelTruncated = truncatePreview(label, labelPreviewBytes)
		args, argsTruncated = argsPreview(args, argsPreviewBytes)
	}
	st := RunStatus{
		RunID: s.ID, State: string(s.State), Label: label, LabelTruncated: labelTruncated,
		Command: s.Command, Executable: s.Executable,
		Args: args, ArgsTruncated: argsTruncated, Cwd: s.Cwd, Env: s.Env, PID: s.PID,
		CreatedAt: ts(&s.CreatedAt), StartedAt: ts(s.StartedAt), EndedAt: ts(s.EndedAt),
		ExitCode: s.ExitCode, Signal: s.Signal, Termination: s.Termination, Error: s.Error,
		TimeoutSeconds: s.TimeoutSeconds, ProcessGroup: s.ProcessGroup, OwnedElsewhere: s.OwnedElsewhere,
		Output: outputStatus(s),
	}
	if st.Args == nil {
		st.Args = []string{}
	}
	if s.StartedAt != nil {
		end := time.Now()
		if s.EndedAt != nil {
			end = *s.EndedAt
		}
		st.DurationSeconds = end.Sub(*s.StartedAt).Seconds()
	}
	return st
}
