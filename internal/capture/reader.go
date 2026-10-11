package capture

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// View selects the lines a reader addresses: one stream, or both in arrival order.
type View uint8

const (
	ViewStdout   View = View(Stdout)
	ViewStderr   View = View(Stderr)
	ViewCombined View = 2
)

func (v View) String() string {
	if v == ViewCombined {
		return "combined"
	}
	return Stream(v).String()
}

// ParseView accepts "stdout", "stderr", "combined" or "" (combined).
func ParseView(s string) (View, error) {
	switch s {
	case "", "combined":
		return ViewCombined, nil
	case "stdout":
		return ViewStdout, nil
	case "stderr":
		return ViewStderr, nil
	}
	return 0, fmt.Errorf("stream must be stdout, stderr or combined, not %q", s)
}

// Line is one line of a view. Data holds at most the requested number of bytes,
// without the '\n' terminator; Length is the full length without it.
type Line struct {
	N          int64
	Stream     Stream
	StreamLine int64
	Offset     int64
	Length     int64
	Terminated bool
	Partial    bool
	Data       []byte
}

// Truncated reports whether Data holds less than the whole line.
func (l Line) Truncated() bool { return int64(len(l.Data)) < l.Length }

// Counts describes what is currently stored.
type Counts struct {
	Bytes        [2]int64
	Lines        [2]int64
	Combined     int64
	PartialBytes [2]int64
}

// ViewLines is the number of complete (indexed) lines in view v.
func (c Counts) ViewLines(v View) int64 {
	if v == ViewCombined {
		return c.Combined
	}
	return c.Lines[v]
}

// Reader reads a run directory's output; it is safe while a Writer appends.
type Reader struct {
	logs   [2]*os.File
	idx    [2]*os.File
	comb   *os.File
	active bool
}

// OpenReader opens dir for reading. active says whether a writer may still
// append, which makes an unindexed tail an in-progress line.
func OpenReader(dir string, active bool) (*Reader, error) {
	r := &Reader{active: active}
	open := func(name string) (*os.File, error) {
		f, err := os.Open(filepath.Join(dir, name))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return f, err
	}
	var err error
	for _, s := range []Stream{Stdout, Stderr} {
		if r.logs[s], err = open(logName(s)); err != nil {
			r.Close()
			return nil, err
		}
		if r.idx[s], err = open(idxName(s)); err != nil {
			r.Close()
			return nil, err
		}
	}
	if r.comb, err = open(combinedName); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// Close releases the reader's files.
func (r *Reader) Close() {
	for _, f := range []*os.File{r.logs[0], r.logs[1], r.idx[0], r.idx[1], r.comb} {
		if f != nil {
			_ = f.Close()
		}
	}
}

func fileSize(f *os.File) (int64, error) {
	if f == nil {
		return 0, nil
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func entryAt(f *os.File, i int64) (uint64, error) {
	if f == nil {
		return 0, io.ErrUnexpectedEOF
	}
	var b [entrySize]byte
	if _, err := f.ReadAt(b[:], i*entrySize); err != nil {
		return 0, fmt.Errorf("reading index entry %d: %w", i, err)
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

// Counts reports the stored bytes and lines. Index sizes are read before log
// sizes so every counted line's bytes are included.
func (r *Reader) Counts() (Counts, error) {
	var c Counts
	csize, err := fileSize(r.comb)
	if err != nil {
		return c, err
	}
	c.Combined = csize / entrySize
	for _, s := range []Stream{Stdout, Stderr} {
		isize, err := fileSize(r.idx[s])
		if err != nil {
			return c, err
		}
		c.Lines[s] = isize / entrySize
		if c.Bytes[s], err = fileSize(r.logs[s]); err != nil {
			return c, err
		}
		var lastEnd int64
		if c.Lines[s] > 0 {
			e, err := entryAt(r.idx[s], c.Lines[s]-1)
			if err != nil {
				return c, err
			}
			lastEnd = int64(e)
		}
		if c.Bytes[s] > lastEnd {
			c.PartialBytes[s] = c.Bytes[s] - lastEnd
		}
	}
	return c, nil
}

func (r *Reader) streamLine(s Stream, k int64, maxBytes int) (Line, error) {
	e, err := entryAt(r.idx[s], k-1)
	if err != nil {
		return Line{}, err
	}
	end := int64(e)
	var start int64
	if k > 1 {
		p, err := entryAt(r.idx[s], k-2)
		if err != nil {
			return Line{}, err
		}
		start = int64(p)
	}
	l := Line{N: k, Stream: s, StreamLine: k, Offset: start, Length: end - start}
	if l.Length > 0 {
		var last [1]byte
		if _, err := r.logs[s].ReadAt(last[:], end-1); err != nil {
			return Line{}, err
		}
		if last[0] == '\n' {
			l.Terminated = true
			l.Length--
		}
	}
	l.Data = make([]byte, min(l.Length, int64(maxBytes)))
	if _, err := r.logs[s].ReadAt(l.Data, start); err != nil && !(errors.Is(err, io.EOF) && len(l.Data) == 0) {
		return Line{}, err
	}
	return l, nil
}

// Line returns complete line n (1-based) of view v.
func (r *Reader) Line(v View, n int64, maxBytes int) (Line, error) {
	if v != ViewCombined {
		return r.streamLine(Stream(v), n, maxBytes)
	}
	e, err := entryAt(r.comb, n-1)
	if err != nil {
		return Line{}, err
	}
	s := Stdout
	if e&stderrBit != 0 {
		s = Stderr
	}
	l, err := r.streamLine(s, int64(e&^stderrBit), maxBytes)
	l.N = n
	return l, err
}

// PartialLine returns stream s's in-progress line, if the capture is active and
// bytes after the last complete line are present.
func (r *Reader) PartialLine(s Stream, c Counts, maxBytes int) (Line, bool, error) {
	if !r.active || c.PartialBytes[s] == 0 {
		return Line{}, false, nil
	}
	start := c.Bytes[s] - c.PartialBytes[s]
	data := make([]byte, min(c.PartialBytes[s], int64(maxBytes)))
	if _, err := r.logs[s].ReadAt(data, start); err != nil {
		return Line{}, false, err
	}
	if bytes.IndexByte(data, '\n') >= 0 {
		// A line completed between the index read and this read; it will be indexed shortly.
		return Line{}, false, nil
	}
	return Line{N: c.Lines[s] + 1, Stream: s, StreamLine: c.Lines[s] + 1, Offset: start,
		Length: c.PartialBytes[s], Partial: true, Data: data}, true, nil
}

// ReadBytes returns up to max bytes of stream s from offset, and the stream's stored size.
func (r *Reader) ReadBytes(s Stream, offset int64, max int) ([]byte, int64, error) {
	size, err := fileSize(r.logs[s])
	if err != nil {
		return nil, 0, err
	}
	if offset >= size {
		return nil, size, nil
	}
	data := make([]byte, min(size-offset, int64(max)))
	if _, err := r.logs[s].ReadAt(data, offset); err != nil {
		return nil, size, err
	}
	return data, size, nil
}

type seqStream struct {
	idx  *bufio.Reader
	log  *bufio.Reader
	pos  int64
	next int64
}

func (r *Reader) seek(s Stream, k int64) (*seqStream, error) {
	var start int64
	if k > 1 {
		e, err := entryAt(r.idx[s], k-2)
		if err != nil {
			return nil, err
		}
		start = int64(e)
	}
	return &seqStream{
		idx:  bufio.NewReaderSize(io.NewSectionReader(r.idx[s], (k-1)*entrySize, 1<<62), 64<<10),
		log:  bufio.NewReaderSize(io.NewSectionReader(r.logs[s], start, 1<<62), 256<<10),
		pos:  start,
		next: k,
	}, nil
}

func (q *seqStream) read(s Stream, maxBytes int, buf []byte) (Line, error) {
	var b [entrySize]byte
	if _, err := io.ReadFull(q.idx, b[:]); err != nil {
		return Line{}, err
	}
	end := int64(binary.BigEndian.Uint64(b[:]))
	n := end - q.pos
	take := min(n, int64(maxBytes))
	data := buf[:take]
	if _, err := io.ReadFull(q.log, data); err != nil {
		return Line{}, err
	}
	var last byte
	if rest := n - take; rest > 0 {
		if _, err := q.log.Discard(int(rest - 1)); err != nil {
			return Line{}, err
		}
		c, err := q.log.ReadByte()
		if err != nil {
			return Line{}, err
		}
		last = c
	} else if take > 0 {
		last = data[take-1]
	}
	l := Line{Stream: s, StreamLine: q.next, Offset: q.pos, Length: n}
	if n > 0 && last == '\n' {
		l.Terminated = true
		l.Length--
		if int64(len(data)) > l.Length {
			data = data[:l.Length]
		}
	}
	l.Data = data
	q.pos = end
	q.next++
	return l, nil
}

// Scan calls fn for complete lines from..to (inclusive) of view v, in order,
// with at most maxBytes of each line. Line.Data is reused between calls. Scan
// stops when fn returns false or ctx is done.
func (r *Reader) Scan(ctx context.Context, v View, from, to int64, maxBytes int, fn func(Line) bool) error {
	if from < 1 || from > to {
		return nil
	}
	bufs := [2][]byte{make([]byte, maxBytes), make([]byte, maxBytes)}
	var seqs [2]*seqStream
	next := func(s Stream, k int64) (Line, error) {
		if seqs[s] == nil || seqs[s].next != k {
			q, err := r.seek(s, k)
			if err != nil {
				return Line{}, err
			}
			seqs[s] = q
		}
		return seqs[s].read(s, maxBytes, bufs[s])
	}
	var comb *bufio.Reader
	if v == ViewCombined {
		comb = bufio.NewReaderSize(io.NewSectionReader(r.comb, (from-1)*entrySize, 1<<62), 64<<10)
	}
	for n := from; n <= to; n++ {
		if n%4096 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		var l Line
		var err error
		if v == ViewCombined {
			var b [entrySize]byte
			if _, err = io.ReadFull(comb, b[:]); err != nil {
				return err
			}
			e := binary.BigEndian.Uint64(b[:])
			s := Stdout
			if e&stderrBit != 0 {
				s = Stderr
			}
			l, err = next(s, int64(e&^stderrBit))
		} else {
			l, err = next(Stream(v), n)
		}
		if err != nil {
			return err
		}
		l.N = n
		if !fn(l) {
			return nil
		}
	}
	return nil
}

// Repair makes the files of a capture whose writer stopped abruptly
// consistent: it drops torn or dangling index entries and indexes each
// stream's unterminated tail as a final line. It reports what is stored.
func Repair(dir string) (Stats, error) {
	var st Stats
	var lines [2]int64
	for _, s := range []Stream{Stdout, Stderr} {
		logSize, err := sizeOf(filepath.Join(dir, logName(s)))
		if err != nil {
			return st, err
		}
		n, lastEnd, err := trimIndex(filepath.Join(dir, idxName(s)), func(e uint64) bool { return int64(e) <= logSize })
		if err != nil {
			return st, err
		}
		if logSize > lastEnd {
			n++
			if err := appendEntry(filepath.Join(dir, idxName(s)), uint64(logSize)); err != nil {
				return st, err
			}
		}
		lines[s] = n
		st.Bytes[s], st.Lines[s] = logSize, n
		st.StoredBytes += logSize
	}
	combPath := filepath.Join(dir, combinedName)
	var seen [2]int64
	if _, _, err := trimIndex(combPath, func(e uint64) bool {
		s := Stdout
		if e&stderrBit != 0 {
			s = Stderr
		}
		k := int64(e &^ stderrBit)
		if k > lines[s] {
			return false
		}
		seen[s] = k
		return true
	}); err != nil {
		return st, err
	}
	for _, s := range []Stream{Stdout, Stderr} {
		for k := seen[s] + 1; k <= lines[s]; k++ {
			if err := appendEntry(combPath, combinedEntry(s, k)); err != nil {
				return st, err
			}
		}
	}
	return st, nil
}

func sizeOf(path string) (int64, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// trimIndex truncates an index to whole entries, the leading run of which ok
// accepts, and returns their number and the last accepted entry.
func trimIndex(path string, ok func(uint64) bool) (int64, int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var n int64
	var last uint64
	for ; (n+1)*entrySize <= int64(len(data)); n++ {
		e := binary.BigEndian.Uint64(data[n*entrySize:])
		if !ok(e) {
			break
		}
		last = e
	}
	if n*entrySize != int64(len(data)) {
		if err := os.Truncate(path, n*entrySize); err != nil {
			return 0, 0, err
		}
	}
	return n, int64(last), nil
}

func appendEntry(path string, e uint64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(binary.BigEndian.AppendUint64(nil, e))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
