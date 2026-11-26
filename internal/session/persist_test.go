package session

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func tmpState(t *testing.T) {
	t.Setenv("STATE_DIR", t.TempDir())
	t.Setenv("STATE_PERSIST", "")
	t.Setenv("STATE_SAVE_SEC", "")
	t.Setenv("STATE_MAX_AGE_HOURS", "")
}

func TestJobSurvivesRestart(t *testing.T) {
	tmpState(t)
	started := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)

	s := NewSession()
	s.JobInProgress = true
	s.JobStartedAt = started
	s.ProcessedPayloadsMap["operator"] = map[string]any{"operator": "豊住"}
	s.ProcessedPayloadsMap["product_code"] = map[string]any{"product_code": int64(61890891)}
	s.ProcessedPayloadsMap["total_output"] = map[string]any{"total_output": 453.0}
	s.SaveState("job_m100,job")

	// "restart": a brand new session loads the file
	r := NewSession()
	r.loadState("job_m100,job")
	if !r.JobInProgress || !r.JobStartedAt.Equal(started) {
		t.Fatalf("job not restored: in_progress=%v started=%v", r.JobInProgress, r.JobStartedAt)
	}
	if got := r.ProcessedPayloadsMap["operator"]["operator"]; got != "豊住" {
		t.Fatalf("operator = %v", got)
	}
	if got := r.ProcessedPayloadsMap["product_code"]["product_code"]; got != int64(61890891) {
		t.Fatalf("product_code = %v (%T), want int64 61890891", got, got)
	}
}

func TestFileDeletedWhenNothingToKeep(t *testing.T) {
	tmpState(t)
	s := NewSession()
	s.JobInProgress = true
	s.SaveState("k")
	path := stateFile("k")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	s.JobInProgress = false // job finished
	s.SaveState("k")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("state file still there after the job ended (err=%v)", err)
	}
}

func TestNoFileWhenIdle(t *testing.T) {
	tmpState(t)
	s := NewSession()
	s.SaveState("idle")
	if _, err := os.Stat(stateFile("idle")); !os.IsNotExist(err) {
		t.Fatal("state file written for an idle session")
	}
}

func TestStaleFileIgnoredAndDeleted(t *testing.T) {
	tmpState(t)
	p := persisted{SavedAt: time.Now().Add(-30 * time.Hour), JobInProgress: true}
	if err := writeFileAtomic(stateFile("old"), p); err != nil {
		t.Fatal(err)
	}
	s := NewSession()
	s.loadState("old")
	if s.JobInProgress {
		t.Fatal("30 h old state was restored")
	}
	if _, err := os.Stat(stateFile("old")); !os.IsNotExist(err) {
		t.Fatal("stale file not deleted")
	}
}

func TestBrokenFileDeleted(t *testing.T) {
	tmpState(t)
	path := stateFile("bad")
	os.MkdirAll(StateDir(), 0o755)
	os.WriteFile(path, []byte("{not json"), 0o644)
	NewSession().loadState("bad")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("broken file not deleted")
	}
}

func TestRoutineChangesAreThrottled(t *testing.T) {
	tmpState(t)
	t.Setenv("STATE_SAVE_SEC", "3600")
	s := NewSession()
	s.JobInProgress = true
	s.ProcessedPayloadsMap["total_output"] = map[string]any{"total_output": 1.0}
	s.SaveState("thr") // job start: important -> written
	s.ProcessedPayloadsMap["total_output"] = map[string]any{"total_output": 2.0}
	s.SaveState("thr") // counter only: throttled

	raw, _ := os.ReadFile(stateFile("thr"))
	var p persisted
	json.Unmarshal(raw, &p)
	if got := p.JobFields["total_output"]["total_output"]; got != 1.0 {
		t.Fatalf("counter change written immediately (got %v), want throttled", got)
	}
	s.JobInProgress = false // job end: important -> handled at once (file removed)
	s.SaveState("thr")
	if _, err := os.Stat(stateFile("thr")); !os.IsNotExist(err) {
		t.Fatal("job end not applied immediately")
	}
}

func TestLotLatchesSurvive(t *testing.T) {
	tmpState(t)
	s := NewSession()
	s.LotCondMet = map[string]bool{"d418 > 400": true}
	s.LotResetHave, s.LotResetLast = true, 8127
	s.SaveState("lot")
	r := NewSession()
	r.loadState("lot")
	if !r.LotCondMet["d418 > 400"] || r.LotResetLast != 8127 {
		t.Fatalf("lot state not restored: %+v %v", r.LotCondMet, r.LotResetLast)
	}
}

func TestPersistOff(t *testing.T) {
	tmpState(t)
	t.Setenv("STATE_PERSIST", "false")
	s := NewSession()
	s.JobInProgress = true
	s.SaveState("off")
	if _, err := os.Stat(stateFile("off")); !os.IsNotExist(err) {
		t.Fatal("file written with STATE_PERSIST=false")
	}
}
