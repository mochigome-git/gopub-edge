package handler

import (
	"testing"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// jobHarness drives handleJobCase frame by frame with a fake clock and
// records what would be posted.
type jobHarness struct {
	t      *testing.T
	sess   *session.Session
	tk     utils.TriggerKey
	now    time.Time
	posted []map[string]map[string]any
}

func newJobHarness(t *testing.T) *jobHarness {
	t.Setenv("CASE_JOB_room_temp", "w69")
	t.Setenv("CASE_JOB_total_output", "d500")
	t.Setenv("JOB_END_DEBOUNCE_FRAMES", "3")
	t.Setenv("JOB_END_DEBOUNCE_SEC", "2")

	h := &jobHarness{
		t:    t,
		sess: &session.Session{ProcessedPayloadsMap: map[string]map[string]any{}},
		tk:   utils.TriggerKey{TriggerKey: "m100", CaseKey: "job"},
		now:  time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC),
	}
	oldNow, oldPost := jobNow, postJob
	jobNow = func() time.Time { return h.now }
	postJob = func(_ *session.Session, frozen map[string]map[string]any, _ []string, _ config.AppConfig, _ <-chan string) {
		h.posted = append(h.posted, frozen)
	}
	t.Cleanup(func() { jobNow, postJob = oldNow, oldPost })
	return h
}

// frame feeds one PLC frame, advancing the fake clock by step first.
func (h *jobHarness) frame(step time.Duration, vals map[string]any) {
	h.now = h.now.Add(step)
	p := utils.NewSafeJsonPayloads()
	for k, v := range vals {
		p.Set(k, v)
	}
	handleJobCase(h.sess, h.tk, p, nil, config.AppConfig{}, nil)
}

func on(temp, total float64) map[string]any {
	return map[string]any{"m100": float64(1), "w69": temp, "d500": total}
}

func off(temp, total float64) map[string]any {
	return map[string]any{"m100": float64(0), "w69": temp, "d500": total}
}

// A short OFF glitch must not end the job: no post, and the same job
// keeps collecting (this is how one job got posted twice on 11-18).
func TestJobEndGlitchDoesNotSplitJob(t *testing.T) {
	h := newJobHarness(t)
	h.frame(0, on(233, 10))
	h.frame(time.Second, off(0, 0)) // glitch frame
	h.frame(time.Second, on(233, 11))
	if len(h.posted) != 0 {
		t.Fatalf("glitch posted %d job(s), want 0", len(h.posted))
	}
	if !h.sess.JobInProgress || h.sess.JobEnding {
		t.Fatalf("after glitch: JobInProgress=%v JobEnding=%v, want true/false", h.sess.JobInProgress, h.sess.JobEnding)
	}

	// real end: needs >= 3 OFF frames AND >= 2s
	h.frame(time.Second, off(233, 11))
	h.frame(500*time.Millisecond, off(233, 11))
	h.frame(500*time.Millisecond, off(233, 11)) // 3 frames but only 1s
	if len(h.posted) != 0 {
		t.Fatalf("posted before debounce time elapsed")
	}
	h.frame(time.Second, off(233, 11)) // 2s
	if len(h.posted) != 1 {
		t.Fatalf("posted %d job(s) after confirmed end, want 1", len(h.posted))
	}
	if got := h.posted[0]["total_output"]["total_output"]; got != float64(11) {
		t.Errorf("total_output = %v, want 11", got)
	}
	if h.sess.JobInProgress || len(h.sess.ProcessedPayloadsMap) != 0 {
		t.Errorf("session not reset for the next job")
	}
}

// Once the trigger drops, the held data is frozen: the PLC resetting or
// changing registers during the debounce window must not reach the post.
func TestJobEndFreezesHeldDataWhileEnding(t *testing.T) {
	h := newJobHarness(t)
	h.frame(0, on(233, 396))
	h.frame(time.Second, off(233, 396))
	h.frame(time.Second, off(150, 1)) // PLC reset to non-zero defaults
	h.frame(time.Second, off(150, 1))

	if len(h.posted) != 1 {
		t.Fatalf("posted %d job(s), want 1", len(h.posted))
	}
	got := h.posted[0]
	if v := got["room_temp"]["room_temp"]; v != float64(233) {
		t.Errorf("room_temp = %v, want 233 (value from before the trigger dropped)", v)
	}
	if v := got["total_output"]["total_output"]; v != float64(396) {
		t.Errorf("total_output = %v, want 396", v)
	}
	// no PLC end time configured -> ended_at is when the trigger dropped
	if v := got["ended_at"]["ended_at"]; v != "2026-11-19T08:00:01Z" {
		t.Errorf("ended_at = %v, want 2026-11-19T08:00:01Z", v)
	}
}

// The snapshot handed to the publisher must not be the session's map, so
// a new job starting while the post is built can't change it.
func TestJobEndPostsSnapshotNotSessionMap(t *testing.T) {
	h := newJobHarness(t)
	t.Setenv("JOB_END_DEBOUNCE_FRAMES", "1")
	t.Setenv("JOB_END_DEBOUNCE_SEC", "0")
	h.frame(0, on(233, 5))
	h.frame(time.Second, off(0, 0))
	if len(h.posted) != 1 {
		t.Fatalf("posted %d job(s), want 1", len(h.posted))
	}
	h.frame(time.Second, on(199, 1)) // next job starts
	if v := h.posted[0]["room_temp"]["room_temp"]; v != float64(233) {
		t.Errorf("posted snapshot changed by next job: room_temp = %v, want 233", v)
	}
}
