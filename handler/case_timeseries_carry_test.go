package handler

import (
	"testing"
	"time"
)

func TestTimeseriesCarryForwardFillsSplitBatch(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "")
	s := &timeseriesSampler{}
	t0 := time.Unix(1000, 0)

	// full cycle, then a batch that only carries part of the registers
	if n, ok := s.mergeBatch(map[string]any{"d0": 1.0, "d1": 2.0, "d2": 3.0}, t0, 9*time.Second, 3); !ok || n != 3 {
		t.Fatalf("first: n=%d ok=%v", n, ok)
	}
	n, ok := s.mergeBatch(map[string]any{"d0": 10.0}, t0.Add(3*time.Second), 9*time.Second, 3)
	if !ok || n != 3 {
		t.Fatalf("split batch rejected: n=%d ok=%v", n, ok)
	}
	if s.latest["d0"] != 10.0 || s.latest["d1"] != 2.0 || s.latest["d2"] != 3.0 {
		t.Fatalf("latest = %v, want d0 new, d1/d2 carried", s.latest)
	}
	if s.seq != 2 {
		t.Fatalf("seq = %d, want 2", s.seq)
	}
}

func TestTimeseriesCarryForwardExpiresStaleRegisters(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "")
	s := &timeseriesSampler{}
	t0 := time.Unix(1000, 0)
	s.mergeBatch(map[string]any{"d0": 1.0, "d1": 2.0}, t0, 9*time.Second, 0)

	// d1 stops arriving: gone once older than max age
	s.mergeBatch(map[string]any{"d0": 1.0}, t0.Add(10*time.Second), 9*time.Second, 0)
	if _, ok := s.latest["d1"]; ok {
		t.Fatalf("stale d1 still sent: %v", s.latest)
	}

	// and MIN_REGISTERS trips when too few are still fresh
	if _, ok := s.mergeBatch(map[string]any{"d0": 1.0}, t0.Add(11*time.Second), 9*time.Second, 2); ok {
		t.Fatal("snapshot with 1 fresh register accepted with min 2")
	}
}

func TestTimeseriesCarryForwardDoesNotRepeatHeldPulse(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "x17")
	s := &timeseriesSampler{}
	t0 := time.Unix(1000, 0)

	s.mergeBatch(map[string]any{"x17": 1.0, "d0": 1.0}, t0, 9*time.Second, 0)
	if s.latest["x17"] != 1.0 {
		t.Fatalf("x17 in its own batch = %v, want 1", s.latest["x17"])
	}
	// next batch has no x17: must not carry the 1 (the hold logic already
	// sends the press once)
	s.mergeBatch(map[string]any{"d0": 2.0}, t0.Add(3*time.Second), 9*time.Second, 0)
	if s.latest["x17"] != float64(0) {
		t.Fatalf("carried x17 = %v, want 0", s.latest["x17"])
	}
}

func TestTimeseriesCarryForwardSwitch(t *testing.T) {
	t.Setenv("TIMESERIES_CARRY_FORWARD", "")
	if !timeseriesCarryForward() {
		t.Fatal("carry-forward should default on")
	}
	t.Setenv("TIMESERIES_CARRY_FORWARD", "false")
	if timeseriesCarryForward() {
		t.Fatal("TIMESERIES_CARRY_FORWARD=false ignored")
	}
}
