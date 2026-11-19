package handler

import (
	"testing"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// A value the PLC showed during the job must survive a later frame where
// the same register reads 0 (ladder/HMI clearing it before the trigger
// drops). Before the fix the last frame won and the job logged 0.
func TestJobCaseKeepsValuesZeroedBeforeTriggerDrops(t *testing.T) {
	t.Setenv("CASE_JOB_room_temp", "w69")
	t.Setenv("CASE_JOB_reject_2", "d900")
	t.Setenv("CASE_JOB_zero_1", "w80")

	sess := &session.Session{ProcessedPayloadsMap: map[string]map[string]any{}}
	tk := utils.TriggerKey{TriggerKey: "m100", CaseKey: "job"}

	frame := func(vals map[string]any) *utils.SafeJsonPayloads {
		p := utils.NewSafeJsonPayloads()
		for k, v := range vals {
			p.Set(k, v)
		}
		return p
	}

	// mid-job: real values
	handleJobCase(sess, tk, frame(map[string]any{"m100": float64(1), "w69": float64(233), "d900": float64(1), "w80": float64(1)}), nil, config.AppConfig{}, nil)
	// last ON frame: registers cleared
	handleJobCase(sess, tk, frame(map[string]any{"m100": float64(1), "w69": float64(0), "d900": float64(0), "w80": float64(0)}), nil, config.AppConfig{}, nil)

	if got := sess.ProcessedPayloadsMap["room_temp"]["room_temp"]; got != float64(233) {
		t.Errorf("room_temp = %v, want 233", got)
	}
	if got := sess.ProcessedPayloadsMap["reject_2"]["reject_2"]; got != float64(1) {
		t.Errorf("reject_2 = %v, want 1", got)
	}
	dc, _ := sess.ProcessedPayloadsMap["daily_check"]["daily_check"].(map[string]any)
	if dc["zero_1"] != float64(1) {
		t.Errorf("daily_check.zero_1 = %v, want 1", dc["zero_1"])
	}

	// a corrected non-zero value still replaces the earlier one
	handleJobCase(sess, tk, frame(map[string]any{"m100": float64(1), "w69": float64(240)}), nil, config.AppConfig{}, nil)
	if got := sess.ProcessedPayloadsMap["room_temp"]["room_temp"]; got != float64(240) {
		t.Errorf("room_temp after correction = %v, want 240", got)
	}
}
