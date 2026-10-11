package capture

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readAll(t *testing.T, dir string, active bool, v View) []Line {
	t.Helper()
	r, err := OpenReader(dir, active)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c, err := r.Counts()
	if err != nil {
		t.Fatal(err)
	}
	var out []Line
	for n := int64(1); n <= c.ViewLines(v); n++ {
		l, err := r.Line(v, n, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, l)
	}
	return out
}

func lineTexts(ls []Line) string {
	var s []string
	for _, l := range ls {
		s = append(s, string(l.Data))
	}
	return strings.Join(s, "|")
}

func TestWriterIndexesLinesAcrossChunks(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(Stdout, []byte("al"))
	w.Write(Stdout, []byte("pha\nbe"))
	w.Write(Stderr, []byte("E1\n"))
	w.Write(Stdout, []byte("ta\n\ngamma"))

	r, _ := OpenReader(dir, true)
	c, _ := r.Counts()
	if c.Lines != [2]int64{3, 1} || c.Combined != 4 || c.PartialBytes[Stdout] != 5 {
		t.Fatalf("counts = %+v", c)
	}
	pl, ok, err := r.PartialLine(Stdout, c, 100)
	if err != nil || !ok || string(pl.Data) != "gamma" || pl.N != 4 {
		t.Fatalf("partial = %+v %v %v", pl, ok, err)
	}
	r.Close()

	w.CloseStream(Stdout)
	w.CloseStream(Stderr)
	<-w.Done()
	if got := lineTexts(readAll(t, dir, false, ViewStdout)); got != "alpha|beta||gamma" {
		t.Fatalf("stdout = %q", got)
	}
	comb := readAll(t, dir, false, ViewCombined)
	if got := lineTexts(comb); got != "alpha|E1|beta||gamma" {
		t.Fatalf("combined = %q", got)
	}
	last := comb[len(comb)-1]
	if last.Terminated || last.Stream != Stdout || last.StreamLine != 4 {
		t.Fatalf("final line = %+v", last)
	}
	if st := w.Stats(); st.Active || st.Lost() != 0 || st.StoredBytes != 20 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestWriterLimitCountsLostBytes(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, 10, nil)
	w.Write(Stdout, []byte("12345678\n"))
	w.Write(Stderr, []byte("abcdef\n"))
	w.Write(Stdout, []byte("more\n"))
	w.CloseStream(Stdout)
	w.CloseStream(Stderr)
	st := w.Stats()
	if !st.LimitReached || st.StoredBytes != 10 || st.LostBytes != [2]int64{5, 6} {
		t.Fatalf("stats = %+v", st)
	}
	if got := lineTexts(readAll(t, dir, false, ViewCombined)); got != "12345678|a" {
		t.Fatalf("combined = %q", got)
	}
}

type failingFile struct {
	File
	failAfter int
	written   int
}

func (f *failingFile) Write(p []byte) (int, error) {
	if f.written+len(p) > f.failAfter {
		n := max(f.failAfter-f.written, 0)
		f.written += n
		_, _ = f.File.Write(p[:n])
		return n, errors.New("no space left on device (injected)")
	}
	f.written += len(p)
	return f.File.Write(p)
}

func TestWriterSurfacesWriteFailure(t *testing.T) {
	dir := t.TempDir()
	open := func(name string) (File, error) {
		f, err := createFile(name)
		if err != nil || filepath.Base(name) != "stdout.log" {
			return f, err
		}
		return &failingFile{File: f, failAfter: 6}, nil
	}
	w, err := NewWriter(dir, 1<<20, open)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(Stdout, []byte("one\ntwo\nthree\n"))
	w.Write(Stdout, []byte("four\n"))
	w.Write(Stderr, []byte("err\n"))
	w.CloseStream(Stdout)
	w.CloseStream(Stderr)
	st := w.Stats()
	if !strings.Contains(st.Failure, "injected") {
		t.Fatalf("failure = %q", st.Failure)
	}
	// 6 of the first 14 bytes were stored; the rest of stdout and all of stderr after the failure are lost.
	if st.Bytes[Stdout] != 6 || st.LostBytes != [2]int64{8 + 5, 4} {
		t.Fatalf("stats = %+v", st)
	}
	if got := lineTexts(readAll(t, dir, false, ViewStdout)); got != "one" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestScanStreamAndCombined(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, 1<<20, nil)
	for i := 0; i < 50; i++ {
		w.Write(Stdout, []byte("o\n"))
		if i%5 == 0 {
			w.Write(Stderr, []byte("e "+strings.Repeat("x", 100)+"\n"))
		}
	}
	w.CloseStream(Stdout)
	w.CloseStream(Stderr)
	r, _ := OpenReader(dir, false)
	defer r.Close()
	c, _ := r.Counts()
	var got []string
	err := r.Scan(context.Background(), ViewCombined, 5, c.Combined, 10, func(l Line) bool {
		if l.Stream == Stderr {
			got = append(got, string(l.Data))
			if !l.Truncated() || l.Length != 102 {
				t.Errorf("line %d not truncated: %+v", l.N, l)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 9 || got[0] != "e xxxxxxxx" {
		t.Fatalf("stderr lines from combined 5 = %d %q", len(got), got)
	}
	for _, from := range []int64{1, 17, 50} {
		var lines []int64
		_ = r.Scan(context.Background(), ViewStdout, from, c.Lines[Stdout], 10, func(l Line) bool {
			lines = append(lines, l.StreamLine)
			return true
		})
		if int64(len(lines)) != 51-from || lines[0] != from {
			t.Fatalf("scan from %d: %v", from, lines)
		}
	}
}

func TestRepairIndexesTornState(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, 1<<20, nil)
	w.Write(Stdout, []byte("a\nb\npartial"))
	w.Write(Stderr, []byte("e\n"))
	// Simulate a crash: a torn index entry, with no CloseStream.
	f, _ := os.OpenFile(filepath.Join(dir, "stdout.idx"), os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write([]byte{0, 0, 1})
	f.Close()
	st, err := Repair(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st.Lines != [2]int64{3, 1} {
		t.Fatalf("stats = %+v", st)
	}
	if got := lineTexts(readAll(t, dir, false, ViewCombined)); got != "a|b|e|partial" {
		t.Fatalf("combined = %q", got)
	}
}
