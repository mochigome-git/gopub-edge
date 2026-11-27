package handler

import (
	"testing"
	"time"
)

func TestCarryForwardFillsSplitBatch(t *testing.T) {
	c := &registerCache{}
	t0 := time.Unix(1000, 0)

	c.fill(lotFrame(map[string]any{"d0": 1.0, "d1": 2.0, "d2": 3.0}), t0, 10*time.Second)

	p := lotFrame(map[string]any{"d0": 10.0})
	if n := c.fill(p, t0.Add(3*time.Second), 10*time.Second); n != 2 {
		t.Fatalf("filled %d, want 2", n)
	}
	got := p.GetData()
	if got["d0"] != 10.0 || got["d1"] != 2.0 || got["d2"] != 3.0 {
		t.Fatalf("payload = %v, want d0 new, d1/d2 carried", got)
	}
	if p.IsCarried("d0") || !p.IsCarried("d1") {
		t.Fatalf("carried marks wrong: d0=%v d1=%v", p.IsCarried("d0"), p.IsCarried("d1"))
	}
}

func TestCarryForwardDropsStaleRegisters(t *testing.T) {
	c := &registerCache{}
	t0 := time.Unix(1000, 0)
	c.fill(lotFrame(map[string]any{"d0": 1.0, "d1": 2.0}), t0, 10*time.Second)

	p := lotFrame(map[string]any{"d0": 1.0})
	c.fill(p, t0.Add(11*time.Second), 10*time.Second)
	if _, ok := p.Get("d1"); ok {
		t.Fatalf("stale d1 carried: %v", p.GetData())
	}
}

// A lot condition register that fell into the previous batch used to be
// "not received" in LOT_CONDITION_MODE=now and the lot was ignored.
func TestCarryForwardLotConditionNowMode(t *testing.T) {
	c := &registerCache{}
	t0 := time.Unix(1000, 0)
	c.fill(lotFrame(map[string]any{"d174": 5.0}), t0, 10*time.Second)

	p := lotFrame(map[string]any{"x26": 1.0}) // trigger batch without d174
	c.fill(p, t0.Add(time.Second), 10*time.Second)
	if v, ok := p.Get("d174"); !ok || v != 5.0 {
		t.Fatalf("d174 = %v %v, want carried 5", v, ok)
	}
}

func TestTimeseriesCarriedHoldOnNotRepeated(t *testing.T) {
	t.Setenv("TIMESERIES_HOLD_ON", "x17")
	s := &timeseriesSampler{}

	// press arrives fresh → held
	p := lotFrame(map[string]any{"x17": 1.0})
	s.noteHeldOn(p)
	if !s.heldOn["x17"] {
		t.Fatal("fresh press not held")
	}
	s.heldOn = nil

	// next batch: x17 only carried (still 1 in the cache) → no new press,
	// and the snapshot sends 0
	p = lotFrame(map[string]any{"d0": 1.0})
	p.SetCarried("x17", 1.0)
	s.noteHeldOn(p)
	if s.heldOn["x17"] {
		t.Fatal("carried x17 counted as a new press")
	}
	snap := map[string]any{"x17": 1.0, "d0": 1.0}
	zeroCarriedHoldOn(snap, p)
	if snap["x17"] != float64(0) {
		t.Fatalf("carried x17 sent as %v, want 0", snap["x17"])
	}
}

func TestCarryForwardSwitch(t *testing.T) {
	t.Setenv("CARRY_FORWARD", "")
	if !carryForwardEnabled() {
		t.Fatal("carry-forward should default on")
	}
	t.Setenv("CARRY_FORWARD", "false")
	if carryForwardEnabled() {
		t.Fatal("CARRY_FORWARD=false ignored")
	}
}
