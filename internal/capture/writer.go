// Package capture stores a command's stdout and stderr exactly as received and
// indexes them by line, so readers can address output by line or byte while it
// is still being written.
//
// A run directory holds, per stream, the raw bytes (stdout.log, stderr.log) and
// the end offset of every line (stdout.idx, stderr.idx: big-endian uint64, one
// per line, offset just past the line's '\n'). combined.idx records the order in
// which Dispatch received lines from either stream: one big-endian uint64 per
// line, the stream in the top bit and the line number in that stream below it.
// Each write appends raw bytes before the index entries that refer to them, so a
// concurrent reader never sees an index entry without its bytes.
package capture

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Stream identifies one output stream of a command.
type Stream uint8

const (
	Stdout Stream = 0
	Stderr Stream = 1
)

func (s Stream) String() string {
	if s == Stderr {
		return "stderr"
	}
	return "stdout"
}

const (
	stderrBit    = uint64(1) << 63
	entrySize    = 8
	combinedName = "combined.idx"
)

func logName(s Stream) string { return s.String() + ".log" }
func idxName(s Stream) string { return s.String() + ".idx" }

func combinedEntry(s Stream, line int64) uint64 {
	e := uint64(line)
	if s == Stderr {
		e |= stderrBit
	}
	return e
}

// File is what the writer needs from an output file; tests substitute failing ones.
type File interface {
	io.Writer
	io.Closer
}

// OpenFunc creates a new output file.
type OpenFunc func(name string) (File, error)

func createFile(name string) (File, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
}

// Stats describes what a capture stored and what it could not store.
type Stats struct {
	Active       bool     `json:"active"`
	MaxBytes     int64    `json:"max_bytes"`
	StoredBytes  int64    `json:"stored_bytes"`
	Bytes        [2]int64 `json:"bytes"`
	Lines        [2]int64 `json:"lines"`
	LostBytes    [2]int64 `json:"lost_bytes"`
	LimitReached bool     `json:"limit_reached"`
	Failure      string   `json:"failure,omitempty"`
}

// Lost is the total number of bytes the command wrote that were not stored.
func (s Stats) Lost() int64 { return s.LostBytes[0] + s.LostBytes[1] }

type streamState struct {
	log, idx  File
	size      int64
	lines     int64
	lineStart int64
	closed    bool
}

// Writer captures both streams of one command into a run directory.
type Writer struct {
	mu       sync.Mutex
	streams  [2]streamState
	combined File
	maxBytes int64
	stored   int64
	lost     [2]int64
	limitHit bool
	failure  string
	open     int
	done     chan struct{}
}

// NewWriter creates the output files in dir. maxBytes bounds the bytes stored
// for both streams together; bytes beyond it are counted as lost, never stored.
func NewWriter(dir string, maxBytes int64, open OpenFunc) (*Writer, error) {
	if open == nil {
		open = createFile
	}
	w := &Writer{maxBytes: maxBytes, open: 2, done: make(chan struct{})}
	var files []File
	fail := func(err error) (*Writer, error) {
		for _, f := range files {
			_ = f.Close()
		}
		return nil, err
	}
	for _, s := range []Stream{Stdout, Stderr} {
		lf, err := open(filepath.Join(dir, logName(s)))
		if err != nil {
			return fail(err)
		}
		files = append(files, lf)
		xf, err := open(filepath.Join(dir, idxName(s)))
		if err != nil {
			return fail(err)
		}
		files = append(files, xf)
		w.streams[s].log, w.streams[s].idx = lf, xf
	}
	cf, err := open(filepath.Join(dir, combinedName))
	if err != nil {
		return fail(err)
	}
	w.combined = cf
	return w, nil
}

// Done is closed when both streams have been closed and the files flushed.
func (w *Writer) Done() <-chan struct{} { return w.done }

// Consume copies r into stream s until r ends, then closes the stream. It keeps
// reading after a failure or the byte limit so the command never blocks on a
// full pipe; such bytes are counted as lost.
func (w *Writer) Consume(s Stream, r io.Reader) {
	buf := make([]byte, 64<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			w.Write(s, buf[:n])
		}
		if err != nil {
			if err != io.EOF && !isClosedPipe(err) {
				w.Fail(fmt.Sprintf("reading %s: %v", s, err))
			}
			break
		}
	}
	w.CloseStream(s)
}

func isClosedPipe(err error) bool { return errors.Is(err, os.ErrClosed) }

// Write stores p as the next bytes of stream s.
func (w *Writer) Write(s Stream, p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) == 0 {
		return
	}
	if w.failure != "" || w.limitHit || w.streams[s].closed {
		w.lost[s] += int64(len(p))
		return
	}
	keep := p
	if room := w.maxBytes - w.stored; int64(len(keep)) > room {
		keep = keep[:room]
		w.lost[s] += int64(len(p)) - room
		w.limitHit = true
	}
	if len(keep) == 0 {
		return
	}
	st := &w.streams[s]
	n, err := st.log.Write(keep)
	base := st.size
	st.size += int64(n)
	w.stored += int64(n)
	if err != nil {
		w.lost[s] += int64(len(keep) - n)
		w.failLocked(fmt.Sprintf("writing %s output: %v", s, err))
	}
	w.indexLocked(s, keep[:n], base)
}

func (w *Writer) indexLocked(s Stream, data []byte, base int64) {
	st := &w.streams[s]
	var idx, comb []byte
	for off := 0; ; {
		i := bytes.IndexByte(data[off:], '\n')
		if i < 0 {
			break
		}
		end := base + int64(off+i+1)
		st.lines++
		st.lineStart = end
		idx = binary.BigEndian.AppendUint64(idx, uint64(end))
		comb = binary.BigEndian.AppendUint64(comb, combinedEntry(s, st.lines))
		off += i + 1
	}
	w.appendIndexLocked(s, idx, comb)
}

func (w *Writer) appendIndexLocked(s Stream, idx, comb []byte) {
	if len(idx) == 0 {
		return
	}
	if _, err := w.streams[s].idx.Write(idx); err != nil {
		w.failLocked(fmt.Sprintf("writing %s line index: %v", s, err))
		return
	}
	if _, err := w.combined.Write(comb); err != nil {
		w.failLocked(fmt.Sprintf("writing combined line index: %v", err))
	}
}

// CloseStream ends stream s. A final line without '\n' is indexed as a line.
func (w *Writer) CloseStream(s Stream) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := &w.streams[s]
	if st.closed {
		return
	}
	st.closed = true
	if w.failure == "" && st.size > st.lineStart {
		st.lines++
		st.lineStart = st.size
		w.appendIndexLocked(s,
			binary.BigEndian.AppendUint64(nil, uint64(st.size)),
			binary.BigEndian.AppendUint64(nil, combinedEntry(s, st.lines)))
	}
	for _, f := range []File{st.log, st.idx} {
		if err := f.Close(); err != nil {
			w.failLocked(fmt.Sprintf("closing %s files: %v", s, err))
		}
	}
	w.open--
	if w.open == 0 {
		if err := w.combined.Close(); err != nil {
			w.failLocked(fmt.Sprintf("closing combined index: %v", err))
		}
		close(w.done)
	}
}

// Fail marks the capture as failed; later bytes are counted as lost.
func (w *Writer) Fail(msg string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failLocked(msg)
}

func (w *Writer) failLocked(msg string) {
	if w.failure == "" {
		w.failure = msg
	}
}

// Stats reports the capture's current state.
func (w *Writer) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := Stats{
		Active:       w.open > 0,
		MaxBytes:     w.maxBytes,
		StoredBytes:  w.stored,
		LostBytes:    w.lost,
		LimitReached: w.limitHit,
		Failure:      w.failure,
	}
	for s := range w.streams {
		st.Bytes[s] = w.streams[s].size
		st.Lines[s] = w.streams[s].lines
	}
	return st
}
