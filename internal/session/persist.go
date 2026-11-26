// persist.go — keep job / lot / machine-state progress across restarts.
//
// Everything a case collects lives in memory; a restart, power cut or
// deploy mid-job used to lose the job's start time and fields, the lot
// latches and momentary state buttons. Each session is now mirrored to
// one small JSON file:
//
//	STATE_DIR=state                 # default ./state (mount it as a volume in Docker)
//	STATE_MAX_AGE_HOURS=24          # older files are ignored and deleted
//	STATE_SAVE_SEC=10               # max write rate for routine changes
//	STATE_PERSIST=false             # turn the whole thing off
//
// File maintenance:
//   - written only when the saved content changed; important changes
//     (job start/end, lot latch, lot created, buttons) at once, routine
//     ones (rising counters) at most every STATE_SAVE_SEC — spares the SD card
//   - written atomically (temp file + rename), so a power cut never leaves
//     a half-written file
//   - DELETED as soon as there is nothing left worth keeping (no job
//     running, no lot latch, ...)
//   - ignored and deleted when older than STATE_MAX_AGE_HOURS or unreadable
package session

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// persisted is the subset of Session that is worth surviving a restart.
// Derived or debounce state (StateKnown, RunActive, ...) is rebuilt from
// live data instead. MachineState is NOT restored on purpose: the state
// machine's first transition after a restart carries no prev_* fields so
// the DB doesn't attribute unobserved downtime to the last state.
type persisted struct {
	SavedAt time.Time `json:"saved_at"`

	// job case
	JobInProgress     bool                      `json:"job_in_progress,omitempty"`
	JobStartedAt      time.Time                 `json:"job_started_at,omitempty"`
	JobEndedAt        time.Time                 `json:"job_ended_at,omitempty"`
	JobStartedFromPLC bool                      `json:"job_started_from_plc,omitempty"`
	JobEndedFromPLC   bool                      `json:"job_ended_from_plc,omitempty"`
	JobFields         map[string]map[string]any `json:"job_fields,omitempty"`
	JobPreStart       map[string]string         `json:"job_pre_start,omitempty"`
	JobFromPreStart   map[string]bool           `json:"job_from_pre_start,omitempty"`

	// lot case
	LotTriggerPrev bool               `json:"lot_trigger_prev,omitempty"`
	LotCondMet     map[string]bool    `json:"lot_cond_met,omitempty"`
	LotCondLast    map[string]float64 `json:"lot_cond_last,omitempty"`
	LotResetLast   float64            `json:"lot_reset_last,omitempty"`
	LotResetHave   bool               `json:"lot_reset_have,omitempty"`
	LotLastCreated time.Time          `json:"lot_last_created,omitempty"`

	// state case — momentary START/STOP and downtime buttons
	RunRequested       bool `json:"run_requested,omitempty"`
	DowntimeRequested  bool `json:"downtime_requested,omitempty"`
	RunStartWasOn      bool `json:"run_start_was_on,omitempty"`
	RunStopWasOn       bool `json:"run_stop_was_on,omitempty"`
	DowntimeStartWasOn bool `json:"downtime_start_was_on,omitempty"`
	DowntimeStopWasOn  bool `json:"downtime_stop_was_on,omitempty"`
}

// lotCooldownKeep: LotLastCreated is only worth keeping this long.
const lotCooldownKeep = 10 * time.Minute

// worthKeeping reports whether p holds anything a restart would lose.
func (p *persisted) worthKeeping(now time.Time) bool {
	return p.JobInProgress || len(p.JobPreStart) > 0 ||
		p.LotTriggerPrev || len(p.LotCondMet) > 0 || p.LotResetHave ||
		(!p.LotLastCreated.IsZero() && now.Sub(p.LotLastCreated) < lotCooldownKeep) ||
		p.RunRequested || p.DowntimeRequested ||
		p.RunStartWasOn || p.RunStopWasOn || p.DowntimeStartWasOn || p.DowntimeStopWasOn
}

// snapshotLocked copies the persisted fields. Caller holds s.Mutex.
func (s *Session) snapshotLocked(now time.Time) persisted {
	p := persisted{
		JobInProgress:     s.JobInProgress,
		JobStartedAt:      s.JobStartedAt,
		JobEndedAt:        s.JobEndedAt,
		JobStartedFromPLC: s.JobStartedFromPLC,
		JobEndedFromPLC:   s.JobEndedFromPLC,
		JobPreStart:       copyMap(s.JobPreStart),
		JobFromPreStart:   copyMap(s.JobFromPreStart),

		LotTriggerPrev: s.LotTriggerPrev,
		LotCondMet:     copyMap(s.LotCondMet),
		LotCondLast:    copyMap(s.LotCondLast),
		LotResetLast:   s.LotResetLast,
		LotResetHave:   s.LotResetHave,

		RunRequested:       s.RunRequested,
		DowntimeRequested:  s.DowntimeRequested,
		RunStartWasOn:      s.RunStartWasOn,
		RunStopWasOn:       s.RunStopWasOn,
		DowntimeStartWasOn: s.DowntimeStartWasOn,
		DowntimeStopWasOn:  s.DowntimeStopWasOn,
	}
	if !s.LotLastCreated.IsZero() && now.Sub(s.LotLastCreated) < lotCooldownKeep {
		p.LotLastCreated = s.LotLastCreated
	}
	// The collected fields only matter while a job is running; the same
	// map holds other cases' scratch data, which is not worth keeping.
	if s.JobInProgress {
		p.JobFields = make(map[string]map[string]any)
		for k, v := range s.ProcessedPayloadsMap {
			if len(v) > 0 {
				p.JobFields[k] = copyMap(v)
			}
		}
	}
	return p
}

// restoreLocked applies p. Caller holds s.Mutex.
func (s *Session) restoreLocked(p persisted) {
	s.JobInProgress = p.JobInProgress
	s.JobStartedAt = p.JobStartedAt
	s.JobEndedAt = p.JobEndedAt
	s.JobStartedFromPLC = p.JobStartedFromPLC
	s.JobEndedFromPLC = p.JobEndedFromPLC
	s.JobPreStart = p.JobPreStart
	s.JobFromPreStart = p.JobFromPreStart
	for k, v := range p.JobFields {
		s.ProcessedPayloadsMap[k] = v
	}

	s.LotTriggerPrev = p.LotTriggerPrev
	s.LotCondMet = p.LotCondMet
	s.LotCondLast = p.LotCondLast
	s.LotResetLast = p.LotResetLast
	s.LotResetHave = p.LotResetHave
	s.LotLastCreated = p.LotLastCreated

	s.RunRequested = p.RunRequested
	s.DowntimeRequested = p.DowntimeRequested
	s.RunStartWasOn = p.RunStartWasOn
	s.RunStopWasOn = p.RunStopWasOn
	s.DowntimeStartWasOn = p.DowntimeStartWasOn
	s.DowntimeStopWasOn = p.DowntimeStopWasOn
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	if len(m) == 0 {
		return nil
	}
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ── files ──────────────────────────────────────────────────────────────

func persistEnabled() bool { return os.Getenv("STATE_PERSIST") != "false" }

// StateDir is where state files and the outbox live.
func StateDir() string {
	if d := strings.TrimSpace(os.Getenv("STATE_DIR")); d != "" {
		return d
	}
	return "state"
}

func envDur(name string, def time.Duration, unit time.Duration) time.Duration {
	if v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(name)), 64); err == nil && v > 0 {
		return time.Duration(v * float64(unit))
	}
	return def
}

func stateMaxAge() time.Duration { return envDur("STATE_MAX_AGE_HOURS", 24*time.Hour, time.Hour) }
func stateSaveEvery() time.Duration {
	return envDur("STATE_SAVE_SEC", 10*time.Second, time.Second)
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func stateFile(caseKey string) string {
	name := strings.Trim(unsafeChars.ReplaceAllString(caseKey, "_"), "_")
	if name == "" {
		name = "default"
	}
	return filepath.Join(StateDir(), "session-"+name+".json")
}

// loadState restores s from its file, if fresh. Stale or broken files
// are deleted.
func (s *Session) loadState(caseKey string) {
	if !persistEnabled() {
		return
	}
	path := stateFile(caseKey)
	raw, err := os.ReadFile(path)
	if err != nil {
		return // no file — nothing to restore
	}
	var p persisted
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep 61890891 an integer, not 6.1890891e+07
	if err := dec.Decode(&p); err != nil {
		log.Printf("[state] ⚠ %s unreadable (%v) — deleted", path, err)
		_ = os.Remove(path)
		return
	}
	if age := time.Since(p.SavedAt); age > stateMaxAge() {
		log.Printf("[state] %s is %s old (> STATE_MAX_AGE_HOURS) — ignored and deleted", path, age.Round(time.Minute))
		_ = os.Remove(path)
		return
	}
	for _, m := range p.JobFields {
		for k, v := range m {
			m[k] = fromJSONNumber(v)
		}
	}
	s.Mutex.Lock()
	s.restoreLocked(p)
	s.persistLastFull, _ = json.Marshal(p.withoutTime())
	s.persistLastKey = p.importantKey()
	s.Mutex.Unlock()

	var what []string
	if p.JobInProgress {
		what = append(what, "job started "+p.JobStartedAt.Local().Format("01-02 15:04:05")+" ("+strconv.Itoa(len(p.JobFields))+" fields)")
	}
	if len(p.LotCondMet) > 0 || p.LotResetHave {
		what = append(what, "lot conditions/baseline")
	}
	if p.RunRequested || p.DowntimeRequested {
		what = append(what, "state buttons")
	}
	log.Printf("[state] ✓ restored from %s (saved %s ago): %s", path,
		time.Since(p.SavedAt).Round(time.Second), strings.Join(what, ", "))
}

func (p persisted) withoutTime() persisted { p.SavedAt = time.Time{}; return p }

// importantKey covers everything except the job's collected field values
// (counters rise every frame) — a change here is saved at once.
func (p persisted) importantKey() string {
	q := p.withoutTime()
	q.JobFields = nil
	q.LotCondLast = nil // last raw values, only for logs
	b, _ := json.Marshal(q)
	return string(b)
}

// SaveState mirrors s to its file if anything changed: important changes
// at once, routine ones at most every STATE_SAVE_SEC. When nothing is worth
// keeping the file is deleted. Call after each processed batch.
func (s *Session) SaveState(caseKey string) {
	if !persistEnabled() {
		return
	}
	now := time.Now()
	s.Mutex.Lock()
	p := s.snapshotLocked(now)
	full, _ := json.Marshal(p)
	key := p.importantKey()
	changed := !bytes.Equal(full, s.persistLastFull)
	important := key != s.persistLastKey
	due := now.Sub(s.persistLastWrite) >= stateSaveEvery()
	if !changed || (!important && !due) {
		s.Mutex.Unlock()
		return
	}
	s.persistLastFull, s.persistLastKey, s.persistLastWrite = full, key, now
	s.Mutex.Unlock()

	path := stateFile(caseKey)
	if !p.worthKeeping(now) {
		if err := os.Remove(path); err == nil {
			log.Printf("[state] nothing left to keep — %s deleted", path)
		}
		return
	}
	p.SavedAt = now.UTC()
	if err := writeFileAtomic(path, p); err != nil {
		log.Printf("[state] ⚠ save %s failed: %v", path, err)
	}
}

// writeFileAtomic writes v as JSON to a temp file and renames it over
// path, so a crash never leaves a half-written file.
func writeFileAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// fromJSONNumber turns json.Number back into int64 (whole numbers) or
// float64, recursing into nested objects (daily_check).
func fromJSONNumber(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	case map[string]any:
		for k, vv := range x {
			x[k] = fromJSONNumber(vv)
		}
		return x
	case []any:
		for i, vv := range x {
			x[i] = fromJSONNumber(vv)
		}
		return x
	}
	return v
}
