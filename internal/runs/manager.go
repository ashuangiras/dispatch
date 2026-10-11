// Package runs starts commands, captures their output, tracks their lifecycle
// and keeps their records across server restarts.
package runs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/ashuangiras/dispatch/internal/capture"
)

// Config bounds what a Manager may use.
type Config struct {
	StateDir         string
	MaxActive        int
	MaxOutputBytes   int64
	MaxRetainedRuns  int
	MaxRetainedBytes int64
	Retention        time.Duration
	DefaultKillGrace time.Duration
	MaxKillGrace     time.Duration
	OrphanGrace      time.Duration
	Environ          func() []string
	Logger           *slog.Logger
}

// Defaults for Config fields left zero.
const (
	DefaultMaxActive        = 16
	DefaultMaxOutputBytes   = 256 << 20
	DefaultMaxRetainedRuns  = 500
	DefaultMaxRetainedBytes = 4 << 30
	DefaultRetention        = 7 * 24 * time.Hour
	DefaultKillGrace        = 5 * time.Second
	DefaultMaxKillGrace     = 60 * time.Second
	DefaultOrphanGrace      = 2 * time.Second
	maxArgBytes             = 1 << 20
)

func (c *Config) setDefaults() {
	if c.MaxActive <= 0 {
		c.MaxActive = DefaultMaxActive
	}
	if c.MaxOutputBytes <= 0 {
		c.MaxOutputBytes = DefaultMaxOutputBytes
	}
	if c.MaxRetainedRuns <= 0 {
		c.MaxRetainedRuns = DefaultMaxRetainedRuns
	}
	if c.MaxRetainedBytes <= 0 {
		c.MaxRetainedBytes = DefaultMaxRetainedBytes
	}
	if c.Retention <= 0 {
		c.Retention = DefaultRetention
	}
	if c.DefaultKillGrace <= 0 {
		c.DefaultKillGrace = DefaultKillGrace
	}
	if c.MaxKillGrace <= 0 {
		c.MaxKillGrace = DefaultMaxKillGrace
	}
	if c.OrphanGrace <= 0 {
		c.OrphanGrace = DefaultOrphanGrace
	}
	if c.Environ == nil {
		c.Environ = os.Environ
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// StartRequest describes a command to run. Args are passed exactly as given;
// nothing is interpreted by a shell unless Command is one.
type StartRequest struct {
	Command        string
	Args           []string
	Cwd            string
	Env            EnvSpec
	Timeout        time.Duration
	KillGrace      time.Duration
	MaxOutputBytes int64
	Label          string
}

// Errors a caller can act on.
var (
	ErrTooManyActive = errors.New("too many active runs")
	ErrShuttingDown  = errors.New("dispatch is shutting down")
	ErrInvalid       = errors.New("invalid request")
	ErrForeign       = errors.New("run is owned by another live Dispatch instance")
)

// Snapshot is a run's record together with its current capture state.
type Snapshot struct {
	Record
	Capture        capture.Stats
	OwnedElsewhere bool
	Dir            string
}

// Manager owns the runs started by this process and reads the others from disk.
type Manager struct {
	cfg      Config
	runsDir  string
	instance string
	mu       sync.Mutex
	active   map[string]*run
	closing  bool
	wg       sync.WaitGroup
	pruneMu  sync.Mutex
}

// New prepares the state directory, marks runs whose owner stopped as
// interrupted, and applies retention.
func New(cfg Config) (*Manager, error) {
	cfg.setDefaults()
	if cfg.StateDir == "" {
		return nil, errors.New("state directory is required")
	}
	m := &Manager{cfg: cfg, runsDir: filepath.Join(cfg.StateDir, "runs"), instance: randomHex(8), active: map[string]*run{}}
	if err := os.MkdirAll(m.runsDir, 0o700); err != nil {
		return nil, err
	}
	if _, err := m.List(""); err != nil {
		return nil, err
	}
	m.prune()
	return m, nil
}

// Config returns the effective configuration.
func (m *Manager) Config() Config { return m.cfg }

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newID() string {
	return "r-" + time.Now().UTC().Format("20060102t150405") + "-" + randomHex(4)
}

type run struct {
	m        *Manager
	dir      string
	mu       sync.Mutex
	rec      Record
	cw       *capture.Writer
	pgid     int
	reason   string
	grace    time.Duration
	started  chan struct{} // closed once pgid is valid, or once launch has failed
	exited   chan struct{}
	done     chan struct{}
	lock     *os.File
	readEnds [2]*os.File
	timer    *time.Timer
}

// Start launches a command. A command that cannot be launched (not found, bad
// working directory, ...) still gets a run, in state failed_to_start; an error
// is returned only for invalid requests and Dispatch's own failures.
func (m *Manager) Start(req StartRequest) (Snapshot, error) {
	if req.Command == "" {
		return Snapshot{}, fmt.Errorf("%w: command is required", ErrInvalid)
	}
	if err := req.Env.validate(); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	n := len(req.Command)
	for _, a := range req.Args {
		n += len(a)
	}
	if n > maxArgBytes {
		return Snapshot{}, fmt.Errorf("%w: command and args total %d bytes, limit %d", ErrInvalid, n, maxArgBytes)
	}
	if req.Timeout < 0 {
		return Snapshot{}, fmt.Errorf("%w: timeout must not be negative", ErrInvalid)
	}
	maxOut := req.MaxOutputBytes
	if maxOut == 0 {
		maxOut = m.cfg.MaxOutputBytes
	}
	if maxOut < 0 || maxOut > m.cfg.MaxOutputBytes {
		return Snapshot{}, fmt.Errorf("%w: max_output_bytes must be between 1 and %d", ErrInvalid, m.cfg.MaxOutputBytes)
	}
	grace := req.KillGrace
	if grace == 0 {
		grace = m.cfg.DefaultKillGrace
	}
	if grace < 0 || grace > m.cfg.MaxKillGrace {
		return Snapshot{}, fmt.Errorf("%w: kill grace must be between 0 and %s", ErrInvalid, m.cfg.MaxKillGrace)
	}

	id := newID()
	r := &run{
		m: m, dir: filepath.Join(m.runsDir, id), grace: grace,
		started: make(chan struct{}), exited: make(chan struct{}), done: make(chan struct{}),
	}

	// published becomes true only once r is visible in m.active with every
	// field snapshot() and terminate() read already initialized. Until then,
	// this defer owns every cleanup path, so a rejected or failed admission
	// never leaves behind a bare run directory, an open lock or a reserved
	// active-count slot (it previously did: r.lock being non-nil after a
	// successful lockOwner skipped the old nil-check cleanup on every later
	// failure, including plain admission rejection).
	published := false
	defer func() {
		if published {
			return
		}
		if r.lock != nil {
			_ = r.lock.Close()
		}
		_ = os.RemoveAll(r.dir)
	}()

	if err := os.Mkdir(r.dir, 0o700); err != nil {
		return Snapshot{}, fmt.Errorf("dispatch could not prepare the run: %w", err)
	}

	// Lock the run directory.
	var err error
	if r.lock, err = lockOwner(r.dir, true); err != nil {
		return Snapshot{}, fmt.Errorf("dispatch could not prepare the run: %w", err)
	}

	// Initialize Record before capture.Writer to ensure consistent state.
	args := append([]string{}, req.Args...)
	r.rec = Record{
		ID: id, Label: req.Label, Command: req.Command, Args: args, Cwd: req.Cwd,
		Env: req.Env.record(), State: StateRunning, CreatedAt: time.Now().UTC(),
		TimeoutSeconds: req.Timeout.Seconds(), KillGraceSeconds: grace.Seconds(),
		Owner: Owner{PID: os.Getpid(), Instance: m.instance},
	}

	// Initialize capture.Writer before publishing.
	if r.cw, err = capture.NewWriter(r.dir, maxOut, nil); err != nil {
		return Snapshot{}, fmt.Errorf("dispatch could not prepare the run: %w", err)
	}

	// Now that critical fields are initialized, check if we can add to m.active.
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return Snapshot{}, ErrShuttingDown
	}
	if len(m.active) >= m.cfg.MaxActive {
		m.mu.Unlock()
		return Snapshot{}, fmt.Errorf("%w: %d runs are active, the limit is %d; wait for or cancel one", ErrTooManyActive, len(m.active), m.cfg.MaxActive)
	}
	m.active[id] = r
	m.wg.Add(1)
	m.mu.Unlock()
	published = true

	// Define abandon function for use after m.active publication.
	abandon := func(err error) (Snapshot, error) {
		close(r.started)
		_ = r.lock.Close()
		_ = os.RemoveAll(r.dir)
		m.mu.Lock()
		delete(m.active, id)
		m.mu.Unlock()
		m.wg.Done()
		return Snapshot{}, fmt.Errorf("dispatch could not prepare the run: %w", err)
	}
	if err := writeRecord(r.dir, r.rec); err != nil {
		return abandon(err)
	}
	if err := r.launch(req); err != nil {
		now := time.Now().UTC()
		r.cw.CloseStream(capture.Stdout)
		r.cw.CloseStream(capture.Stderr)
		st := r.cw.Stats()
		r.mu.Lock()
		r.rec.State = StateFailedToStart
		r.rec.Error = err.Error()
		r.rec.EndedAt = &now
		r.rec.Capture = &st
		r.mu.Unlock()
		close(r.started)
		close(r.exited)
		r.finish()
	}
	return r.snapshot(), nil
}

func (r *run) launch(req StartRequest) error {
	cwd, err := resolveCwd(req.Cwd)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.rec.Cwd = cwd
	r.mu.Unlock()
	env := req.Env.build(r.m.cfg.Environ())
	exe, err := resolveExecutable(req.Command, cwd, env)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.rec.Executable = exe
	r.mu.Unlock()
	var rEnds, wEnds [2]*os.File
	for i := range rEnds {
		if rEnds[i], wEnds[i], err = os.Pipe(); err != nil {
			for j := 0; j < i; j++ {
				_ = rEnds[j].Close()
				_ = wEnds[j].Close()
			}
			return fmt.Errorf("creating output pipe: %w", err)
		}
	}
	cmd := &exec.Cmd{
		Path:        exe,
		Args:        append([]string{req.Command}, req.Args...),
		Dir:         cwd,
		Env:         env,
		Stdout:      wEnds[0],
		Stderr:      wEnds[1],
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	err = cmd.Start()
	for _, f := range wEnds {
		_ = f.Close()
	}
	if err != nil {
		for _, f := range rEnds {
			_ = f.Close()
		}
		return err
	}
	now := time.Now().UTC()
	r.mu.Lock()
	r.pgid = cmd.Process.Pid
	r.rec.PID = cmd.Process.Pid
	r.rec.StartedAt = &now
	r.readEnds = rEnds
	rec := r.rec
	r.mu.Unlock()
	// From here on r.pgid is valid and never changes again: safe to signal it.
	close(r.started)
	if err := writeRecord(r.dir, rec); err != nil {
		r.m.cfg.Logger.Error("persisting run record", "run", rec.ID, "err", err)
	}
	go r.cw.Consume(capture.Stdout, rEnds[0])
	go r.cw.Consume(capture.Stderr, rEnds[1])
	if req.Timeout > 0 {
		r.timer = time.AfterFunc(req.Timeout, func() { r.terminate(TermTimeout, r.grace) })
	}
	go r.monitor(cmd)
	return nil
}

func (r *run) monitor(cmd *exec.Cmd) {
	_ = cmd.Wait()
	ended := time.Now().UTC()
	close(r.exited)
	if r.timer != nil {
		r.timer.Stop()
	}
	note := r.cleanupGroup()
	if !waitFor(r.cw.Done(), 2*time.Second) {
		r.cw.Fail("output pipes were still open after the run's process group ended (held by a process that left the group); Dispatch closed them, so later output was not captured")
		for _, f := range r.readEnds {
			_ = f.Close()
		}
		<-r.cw.Done()
	}
	for _, f := range r.readEnds {
		_ = f.Close()
	}
	st := r.cw.Stats()
	r.mu.Lock()
	r.rec.State = StateExited
	r.rec.EndedAt = &ended
	r.rec.ProcessGroup = note
	r.rec.Capture = &st
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		r.rec.Signal = signalName(ws.Signal())
		r.rec.Termination = TermSignal
	} else {
		code := cmd.ProcessState.ExitCode()
		r.rec.ExitCode = &code
		r.rec.Termination = TermExit
	}
	if r.reason != "" {
		r.rec.Termination = r.reason
	}
	r.mu.Unlock()
	r.finish()
}

// cleanupGroup ends what remains of the run's process group once its leader
// has exited: the run owns its group, so no member outlives the run.
func (r *run) cleanupGroup() string {
	deadline := time.Now().Add(r.m.cfg.OrphanGrace)
	for groupAlive(r.pgid) || !isDone(r.cw.Done()) {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !groupAlive(r.pgid) {
		return ""
	}
	_ = syscall.Kill(-r.pgid, syscall.SIGTERM)
	if waitGroupGone(r.pgid, r.grace) {
		return "process-group members still running after the command exited were terminated with SIGTERM"
	}
	_ = syscall.Kill(-r.pgid, syscall.SIGKILL)
	waitGroupGone(r.pgid, 2*time.Second)
	return "process-group members still running after the command exited were killed with SIGKILL"
}

func groupAlive(pgid int) bool { return pgid > 0 && syscall.Kill(-pgid, 0) == nil }

func waitGroupGone(pgid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for groupAlive(pgid) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

func isDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func waitFor(ch <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return true
	case <-t.C:
		return false
	}
}

// terminate sends SIGTERM to the run's process group, then SIGKILL if the
// leader has not exited after grace. The reason is kept only if the leader was
// still running when termination was requested.
func (r *run) terminate(reason string, grace time.Duration) {
	if isDone(r.exited) {
		return
	}
	r.mu.Lock()
	if r.reason == "" {
		r.reason = reason
	}
	r.mu.Unlock()
	// The run may still be inside Start()/launch(), before a process (and so a
	// process-group ID) exists. Wait for whichever of launch()'s two outcomes
	// comes first instead of reading pgid now: its zero value is not a process
	// group, and signaling it would reach this server's own process group
	// (kill(2) treats pgid 0 as "the caller's"), not the run's.
	select {
	case <-r.started:
	case <-r.exited:
		return
	}
	r.mu.Lock()
	pgid := r.pgid
	r.mu.Unlock()
	if pgid <= 0 {
		// launch failed before starting a process: nothing to signal.
		return
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-r.exited:
	case <-t.C:
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
}

func (r *run) finish() {
	r.mu.Lock()
	rec := r.rec
	r.mu.Unlock()
	if err := writeRecord(r.dir, rec); err != nil {
		r.m.cfg.Logger.Error("persisting run record", "run", rec.ID, "err", err)
	}
	_ = r.lock.Close()
	r.m.mu.Lock()
	delete(r.m.active, rec.ID)
	r.m.mu.Unlock()
	close(r.done)
	r.m.wg.Done()
	go r.m.prune()
}

func (r *run) snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Snapshot{Record: r.rec, Dir: r.dir}
	s.Args = append([]string{}, r.rec.Args...)
	if r.rec.Capture != nil {
		s.Capture = *r.rec.Capture
	} else {
		s.Capture = r.cw.Stats()
	}
	return s
}

// Get returns a run's current snapshot.
func (m *Manager) Get(id string) (Snapshot, error) {
	if err := checkID(id); err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	r := m.active[id]
	m.mu.Unlock()
	if r != nil {
		return r.snapshot(), nil
	}
	return m.load(id)
}

func (m *Manager) load(id string) (Snapshot, error) {
	dir := filepath.Join(m.runsDir, id)
	rec, err := readRecord(dir)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading run %s: %w", id, err)
	}
	if rec.Finished() {
		return finishedSnapshot(rec, dir), nil
	}
	lock, err := lockOwner(dir, false)
	if err != nil {
		m.mu.Lock()
		r := m.active[id]
		m.mu.Unlock()
		if r != nil {
			return r.snapshot(), nil
		}
		if rec2, err2 := readRecord(dir); err2 == nil && rec2.Finished() {
			return finishedSnapshot(rec2, dir), nil
		}
		return foreignSnapshot(rec, dir), nil
	}
	defer lock.Close()
	if rec, err = readRecord(dir); err != nil {
		return Snapshot{}, err
	}
	if rec.Finished() {
		return finishedSnapshot(rec, dir), nil
	}
	st, err := capture.Repair(dir)
	if err != nil {
		st.Failure = "repairing capture files: " + err.Error()
	} else {
		st.Failure = fmt.Sprintf("capture stopped when the owning Dispatch instance (pid %d) stopped; output written after that was not captured", rec.Owner.PID)
	}
	rec.State = StateInterrupted
	rec.Termination = TermUnknown
	rec.Error = fmt.Sprintf("the Dispatch instance (pid %d) that owned this run stopped before the run finished; the command's outcome is unknown. "+
		"Its processes (last pid %d) may or may not still exist: Dispatch does not signal them after a restart because the pid may now belong to another process", rec.Owner.PID, rec.PID)
	rec.Capture = &st
	if err := writeRecord(dir, rec); err != nil {
		return Snapshot{}, err
	}
	return finishedSnapshot(rec, dir), nil
}

func finishedSnapshot(rec Record, dir string) Snapshot {
	s := Snapshot{Record: rec, Dir: dir}
	if rec.Capture != nil {
		s.Capture = *rec.Capture
	}
	return s
}

func foreignSnapshot(rec Record, dir string) Snapshot {
	s := Snapshot{Record: rec, Dir: dir, OwnedElsewhere: true}
	// For foreign instances, we can read what's visible in capture files
	// (bytes and lines) but cannot determine lost bytes or whether the limit was
	// reached, as those are only tracked by the owning instance's Writer.
	if rd, err := capture.OpenReader(dir, true); err == nil {
		if c, err := rd.Counts(); err == nil {
			s.Capture.Bytes = c.Bytes
			s.Capture.Lines = c.Lines
			s.Capture.StoredBytes = c.Bytes[0] + c.Bytes[1]
		}
		rd.Close()
	}
	// Set Active=true if the run is still running; the owning instance will report
	// actual lost_bytes and limit_reached. For finished runs, Active=false.
	s.Capture.Active = rec.State == StateRunning
	// Copy MaxBytes from the record if available
	if rec.Capture != nil {
		s.Capture.MaxBytes = rec.Capture.MaxBytes
	}
	// lost_bytes and limit_reached are tracked only by the owning instance's
	// in-memory Writer: mark them explicitly unknown rather than asserting
	// zero/false, which would look like "nothing was lost" when the truth is
	// simply not visible from here. This is distinct from an actual capture
	// failure (Failure stays empty unless something genuinely went wrong).
	s.Capture.StatsUnknown = true
	return s
}

// Wait blocks until the run finishes, d elapses or ctx is done, and reports
// whether the run finished. It never stops the run.
func (m *Manager) Wait(ctx context.Context, id string, d time.Duration) (Snapshot, bool, error) {
	if err := checkID(id); err != nil {
		return Snapshot{}, false, err
	}
	t := time.NewTimer(d)
	defer t.Stop()
	m.mu.Lock()
	r := m.active[id]
	m.mu.Unlock()
	if r != nil {
		select {
		case <-r.done:
		case <-t.C:
		case <-ctx.Done():
		}
		s := r.snapshot()
		return s, s.Finished(), nil
	}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		s, err := m.Get(id)
		if err != nil || s.Finished() {
			return s, err == nil, err
		}
		select {
		case <-tick.C:
		case <-t.C:
			return s, false, nil
		case <-ctx.Done():
			return s, false, nil
		}
	}
}

// Cancel terminates a running run's process group (SIGTERM, then SIGKILL after
// grace) and waits briefly for the run to finish. Cancelling a finished run is
// a no-op that returns its snapshot.
func (m *Manager) Cancel(id string, grace time.Duration) (Snapshot, error) {
	if err := checkID(id); err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	r := m.active[id]
	m.mu.Unlock()
	if r == nil {
		s, err := m.Get(id)
		if err == nil && s.OwnedElsewhere {
			return s, fmt.Errorf("%w (pid %d); cancel it through that instance", ErrForeign, s.Owner.PID)
		}
		return s, err
	}
	if grace <= 0 {
		grace = r.grace
	}
	if grace > m.cfg.MaxKillGrace {
		return Snapshot{}, fmt.Errorf("%w: grace must be at most %s", ErrInvalid, m.cfg.MaxKillGrace)
	}
	r.terminate(TermCancelled, grace)
	waitFor(r.done, r.m.cfg.OrphanGrace+grace+5*time.Second)
	return r.snapshot(), nil
}

// List returns every run's snapshot, newest first, optionally only one state.
func (m *Manager) List(state State) ([]Snapshot, error) {
	entries, err := os.ReadDir(m.runsDir)
	if err != nil {
		return nil, err
	}
	var out []Snapshot
	for _, e := range entries {
		if !e.IsDir() || checkID(e.Name()) != nil {
			continue
		}
		s, err := m.Get(e.Name())
		if err != nil {
			continue
		}
		if state == "" || s.State == state {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

// Delete removes a finished run's record and output. Running runs are refused.
func (m *Manager) Delete(id string) (int64, error) {
	s, err := m.Get(id)
	if err != nil {
		return 0, err
	}
	if !s.Finished() {
		return 0, fmt.Errorf("%w: run %s is still running; cancel it or wait for it first", ErrInvalid, id)
	}
	n := dirBytes(s.Dir)
	return n, os.RemoveAll(s.Dir)
}

// prune deletes the oldest finished runs beyond the retention limits.
func (m *Manager) prune() {
	m.pruneMu.Lock()
	defer m.pruneMu.Unlock()
	all, err := m.List("")
	if err != nil {
		return
	}
	var total int64
	sizes := make([]int64, len(all))
	for i, s := range all {
		sizes[i] = dirBytes(s.Dir)
		total += sizes[i]
	}
	count := len(all)
	cutoff := time.Now().Add(-m.cfg.Retention)
	for i := len(all) - 1; i >= 0; i-- {
		s := all[i]
		if !s.Finished() {
			continue
		}
		ended := s.CreatedAt
		if s.EndedAt != nil {
			ended = *s.EndedAt
		}
		if count <= m.cfg.MaxRetainedRuns && total <= m.cfg.MaxRetainedBytes && ended.After(cutoff) {
			continue
		}
		if err := os.RemoveAll(s.Dir); err == nil {
			m.cfg.Logger.Info("retention removed run", "run", s.ID)
			count--
			total -= sizes[i]
		}
	}
}

// Shutdown terminates every active run (reason server_shutdown) and waits for
// their records to be written, or for ctx.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	m.closing = true
	var active []*run
	for _, r := range m.active {
		active = append(active, r)
	}
	m.mu.Unlock()
	for _, r := range active {
		go r.terminate(TermServerShutdown, r.grace)
	}
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

var signalNames = map[syscall.Signal]string{
	syscall.SIGHUP: "SIGHUP", syscall.SIGINT: "SIGINT", syscall.SIGQUIT: "SIGQUIT",
	syscall.SIGILL: "SIGILL", syscall.SIGTRAP: "SIGTRAP", syscall.SIGABRT: "SIGABRT",
	syscall.SIGBUS: "SIGBUS", syscall.SIGFPE: "SIGFPE", syscall.SIGKILL: "SIGKILL",
	syscall.SIGUSR1: "SIGUSR1", syscall.SIGSEGV: "SIGSEGV", syscall.SIGUSR2: "SIGUSR2",
	syscall.SIGPIPE: "SIGPIPE", syscall.SIGALRM: "SIGALRM", syscall.SIGTERM: "SIGTERM",
}

func signalName(s syscall.Signal) string {
	if n, ok := signalNames[s]; ok {
		return n
	}
	return fmt.Sprintf("signal %d", int(s))
}
