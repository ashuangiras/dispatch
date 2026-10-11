package runs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/ashuangiras/dispatch/internal/capture"
)

// State is a run's lifecycle state.
type State string

const (
	// StateRunning: the command started and its process group has not been fully reaped.
	StateRunning State = "running"
	// StateExited: the command ended; ExitCode or Signal says how.
	StateExited State = "exited"
	// StateFailedToStart: the command never ran; Error says why.
	StateFailedToStart State = "failed_to_start"
	// StateInterrupted: the Dispatch instance that owned the run stopped before it finished.
	StateInterrupted State = "interrupted"
)

// Termination says why a run ended.
const (
	TermExit           = "exit"
	TermSignal         = "signal"
	TermTimeout        = "timeout"
	TermCancelled      = "cancelled"
	TermServerShutdown = "server_shutdown"
	TermUnknown        = "unknown"
)

// Owner identifies the Dispatch instance that runs a command.
type Owner struct {
	PID      int    `json:"pid"`
	Instance string `json:"instance"`
}

// Record is a run's persisted metadata (run.json). It never holds environment values.
type Record struct {
	ID               string         `json:"id"`
	Label            string         `json:"label,omitempty"`
	Command          string         `json:"command"`
	Executable       string         `json:"executable,omitempty"`
	Args             []string       `json:"args"`
	Cwd              string         `json:"cwd"`
	Env              EnvRecord      `json:"env"`
	State            State          `json:"state"`
	CreatedAt        time.Time      `json:"created_at"`
	StartedAt        *time.Time     `json:"started_at,omitempty"`
	EndedAt          *time.Time     `json:"ended_at,omitempty"`
	PID              int            `json:"pid,omitempty"`
	ExitCode         *int           `json:"exit_code,omitempty"`
	Signal           string         `json:"signal,omitempty"`
	Termination      string         `json:"termination,omitempty"`
	Error            string         `json:"error,omitempty"`
	TimeoutSeconds   float64        `json:"timeout_seconds,omitempty"`
	KillGraceSeconds float64        `json:"kill_grace_seconds"`
	ProcessGroup     string         `json:"process_group,omitempty"`
	Capture          *capture.Stats `json:"capture,omitempty"`
	Owner            Owner          `json:"owner"`
}

// Finished reports whether the run can no longer change.
func (r Record) Finished() bool { return r.State != StateRunning }

var idPattern = regexp.MustCompile(`^r-[0-9]{8}t[0-9]{6}-[0-9a-f]{8}$`)

// ErrNotFound is returned for unknown or malformed run IDs.
var ErrNotFound = errors.New("run not found")

func checkID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: %q is not a run ID (expected r-YYYYMMDDtHHMMSS-xxxxxxxx)", ErrNotFound, id)
	}
	return nil
}

const (
	recordName = "run.json"
	lockName   = "owner.lock"
)

func writeRecord(dir string, r Record) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, recordName+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, recordName))
}

func readRecord(dir string) (Record, error) {
	var r Record
	data, err := os.ReadFile(filepath.Join(dir, recordName))
	if err != nil {
		return r, err
	}
	return r, json.Unmarshal(data, &r)
}

// lockOwner takes the run's owner lock. Holding it means "this process is
// capturing the run"; when the holder dies the kernel releases it, which is how
// another instance tells a live run from an interrupted one without trusting a PID.
func lockOwner(dir string, block bool) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	how := syscall.LOCK_EX
	if !block {
		how |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func dirBytes(dir string) int64 {
	var n int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			n += fi.Size()
		}
	}
	return n
}
