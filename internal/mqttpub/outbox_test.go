package mqttpub

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestOutbox(t *testing.T, maxBytes int64) *outbox {
	t.Helper()
	o := &outbox{dir: t.TempDir(), maxBytes: maxBytes}
	if err := o.open(); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOutboxSendsInOrderAndDeletes(t *testing.T) {
	o := newTestOutbox(t, 1<<20)
	for _, s := range []string{"a", "b", "c"} {
		if err := o.put([]byte(s), "job"); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	sent := o.drain(func(b []byte) error { got = append(got, string(b)); return nil })
	if sent != 3 || strings.Join(got, "") != "abc" {
		t.Fatalf("sent %d in order %v, want 3 abc", sent, got)
	}
	if o.pending() != 0 || len(o.list()) != 0 {
		t.Fatalf("outbox not empty after sending: %d pending, %d files", o.pending(), len(o.list()))
	}
}

func TestOutboxKeepsFileWhenSendFails(t *testing.T) {
	o := newTestOutbox(t, 1<<20)
	o.put([]byte("a"), "lot")
	o.put([]byte("b"), "lot")
	calls := 0
	sent := o.drain(func(b []byte) error {
		calls++
		if calls == 2 {
			return errors.New("broker gone")
		}
		return nil
	})
	if sent != 1 || o.pending() != 1 {
		t.Fatalf("sent=%d pending=%d, want 1 1 (failed post kept for retry)", sent, o.pending())
	}
}

func TestOutboxCapDropsReadingsFirst(t *testing.T) {
	o := newTestOutbox(t, 30) // tiny cap: 30 bytes
	o.put([]byte("JOB-0123456789"), "job")      // 14 bytes, oldest
	o.put([]byte("readings-01234"), "readings") // 14
	o.put([]byte("readings-56789"), "readings") // 14 -> over cap
	names := o.list()
	var kinds []string
	for _, n := range names {
		kinds = append(kinds, kindOf(n))
	}
	if strings.Join(kinds, ",") != "job,readings" {
		t.Fatalf("after cap: %v, want the job kept and the OLDEST readings dropped", kinds)
	}
}

func TestOutboxCountsExistingAndCleansTemp(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "00000000000000000001-00000001-job.json"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "00000000000000000002-00000002-job.json.tmp"), []byte("half"), 0o644)
	o := &outbox{dir: dir, maxBytes: 1 << 20}
	if err := o.open(); err != nil {
		t.Fatal(err)
	}
	if o.pending() != 1 {
		t.Fatalf("pending = %d, want 1 (from before restart)", o.pending())
	}
	if _, err := os.Stat(filepath.Join(dir, "00000000000000000002-00000002-job.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("stray temp file not removed")
	}
}

func TestPostKind(t *testing.T) {
	cases := map[string]map[string]any{
		"job":       {"kind": "job"},
		"lot":       {"kind": "lot"},
		"heartbeat": {"status": map[string]any{"kind": "heartbeat"}},
		"state":     {"status": map[string]any{"machine_state": "running"}},
		"readings":  {"readings": map[string]any{"d392": 300.0}},
	}
	for want, data := range cases {
		if got := postKind(data); got != want {
			t.Errorf("postKind(%v) = %s, want %s", data, got, want)
		}
	}
}

func TestOutboxOffByEnv(t *testing.T) {
	t.Setenv("OUTBOX", "false")
	if outboxFromEnv(t.TempDir()) != nil {
		t.Fatal("outbox created with OUTBOX=false")
	}
}
