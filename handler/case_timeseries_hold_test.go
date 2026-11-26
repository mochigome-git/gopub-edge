package handler

import (
	"testing"
)

func TestTimeseriesHoldOnKeepsShortPulse(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "X17")
	s := &timeseriesSampler{}

	// cycles between two publishes: the press is only in the middle one
	s.noteHeldOn(lotFrame(map[string]any{"x17": float64(0)}))
	s.noteHeldOn(lotFrame(map[string]any{"x17": float64(1)}))
	s.noteHeldOn(lotFrame(map[string]any{"x17": float64(0)}))

	data := map[string]any{"x17": float64(0), "d392": "300.000"}
	s.mu.Lock()
	held := s.applyHeldOnLocked(data)
	s.mu.Unlock()
	if data["x17"] != float64(1) || len(held) != 1 || held[0] != "x17" {
		t.Fatalf("x17 = %v held=%v, want 1 [x17]", data["x17"], held)
	}

	// next publish: no press in between -> the real value (0) again
	data = map[string]any{"x17": float64(0)}
	s.mu.Lock()
	held = s.applyHeldOnLocked(data)
	s.mu.Unlock()
	if data["x17"] != float64(0) || held != nil {
		t.Fatalf("after reset x17 = %v held=%v, want 0 nil", data["x17"], held)
	}
}

func TestTimeseriesHoldOnUsesRename(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "x17")
	t.Setenv(timeseriesPrefix+"reset_button", "x17")
	s := &timeseriesSampler{}
	s.noteHeldOn(lotFrame(map[string]any{"x17": "1"}))
	data := map[string]any{"reset_button": float64(0)}
	s.mu.Lock()
	s.applyHeldOnLocked(data)
	s.mu.Unlock()
	if data["reset_button"] != float64(1) {
		t.Fatalf("reset_button = %v, want 1", data["reset_button"])
	}
}

func TestTimeseriesHoldOnOffByDefault(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "")
	s := &timeseriesSampler{}
	s.noteHeldOn(lotFrame(map[string]any{"x17": float64(1)}))
	data := map[string]any{"x17": float64(0)}
	s.mu.Lock()
	s.applyHeldOnLocked(data)
	s.mu.Unlock()
	if data["x17"] != float64(0) {
		t.Fatal("hold applied without TIMESERIES_HOLD_ON")
	}
}
