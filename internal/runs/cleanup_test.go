package runs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ashuangiras/dispatch/internal/capture"
)

type trackedCaptureFile struct {
	*os.File
	mu     sync.Mutex
	closed bool
}

func (f *trackedCaptureFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.File.Close()
}
func (f *trackedCaptureFile) isClosed() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.closed }

func TestPreparationCleanupClosesCaptureFiles(t *testing.T) {
	for _, scenario := range []string{"active_limit", "shutdown", "open_failure", "record_failure"} {
		t.Run(scenario, func(t *testing.T) {
			m, err := New(Config{StateDir: t.TempDir(), MaxActive: 1, DefaultKillGrace: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				m.Shutdown(ctx)
			}()
			if scenario == "active_limit" {
				if _, err := m.Start(StartRequest{Command: "/bin/sleep", Args: []string{"30"}}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "shutdown" {
				m.Shutdown(context.Background())
			}
			var files []*trackedCaptureFile
			m.openCapture = func(name string) (capture.File, error) {
				if scenario == "open_failure" && filepath.Base(name) == "combined.idx" {
					return nil, errors.New("injected capture preparation failure")
				}
				f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
				if err != nil {
					return nil, err
				}
				tracked := &trackedCaptureFile{File: f}
				files = append(files, tracked)
				if scenario == "record_failure" && filepath.Base(name) == "combined.idx" {
					if err := os.Mkdir(filepath.Join(filepath.Dir(name), "run.json.tmp"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				return tracked, nil
			}
			_, err = m.Start(StartRequest{Command: "/bin/true"})
			if err == nil {
				t.Fatal("expected rejection")
			}
			// Register a fallback so even the intentionally failing pre-fix test releases handles.
			defer func() {
				for _, f := range files {
					if !f.isClosed() {
						_ = f.Close()
					}
				}
			}()
			for _, f := range files {
				if !f.isClosed() {
					t.Errorf("capture file left open after %s: %s", scenario, filepath.Base(f.Name()))
				}
			}
			entries, err := os.ReadDir(m.runsDir)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if scenario == "active_limit" {
				want = 1
			}
			if len(entries) != want {
				t.Fatalf("run directories=%d, want %d", len(entries), want)
			}
		})
	}
}

func TestConcurrentAdmissionAndCompletion(t *testing.T) {
	m, err := New(Config{StateDir: t.TempDir(), MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 30; attempt++ {
				_, err := m.Start(StartRequest{Command: "/bin/true"})
				if err != nil && !errors.Is(err, ErrTooManyActive) {
					t.Errorf("unexpected admission error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
