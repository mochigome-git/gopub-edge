package handler

import (
	"testing"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// countLots swaps publishLot for a counter for the duration of the test.
func countLots(t *testing.T) *int {
	t.Helper()
	n := 0
	orig := publishLot
	publishLot = func(*session.Session, string, config.AppConfig, <-chan string) { n++ }
	t.Cleanup(func() { publishLot = orig })
	return &n
}

func lotFrame(vals map[string]any) *utils.SafeJsonPayloads {
	p := utils.NewSafeJsonPayloads()
	for k, v := range vals {
		p.Set(k, v)
	}
	return p
}

func newLotSession() *session.Session {
	return &session.Session{ProcessedPayloadsMap: map[string]map[string]any{}}
}

var lotTK = utils.TriggerKey{TriggerKey: "x26", CaseKey: "lot"}

func TestParseLotCondition(t *testing.T) {
	cases := []struct {
		raw  string
		op   string
		v    float64
		ok   bool
		test float64
		want bool
	}{
		{">1500", ">", 1500, true, 1501, true},
		{"1500", ">", 1500, true, 1500, false},
		{">= 200", ">=", 200, true, 200, true},
		{"<30", "<", 30, true, 29.5, true},
		{"<=30", "<=", 30, true, 31, false},
		{"=1", "==", 1, true, 1, true},
		{"==1", "==", 1, true, 0, false},
		{"!=0", "!=", 0, true, 2, true},
		{`">1500"`, ">", 1500, true, 2000, true},
		{"abc", "", 0, false, 0, false},
		{">", "", 0, false, 0, false},
	}
	for _, c := range cases {
		got, err := parseLotCondition("D418", c.raw)
		if (err == nil) != c.ok {
			t.Errorf("%q: err = %v, want ok=%v", c.raw, err, c.ok)
			continue
		}
		if !c.ok {
			continue
		}
		if got.Op != c.op || got.Value != c.v || got.Register != "d418" {
			t.Errorf("%q: got %+v", c.raw, got)
		}
		if got.eval(c.test) != c.want {
			t.Errorf("%q eval(%v) = %v, want %v", c.raw, c.test, !c.want, c.want)
		}
	}
}

// No CASE_LOT_REQUIRE_* → unchanged behaviour: every OFF->ON edge is a lot.
func TestLotNoConditionsEveryEdge(t *testing.T) {
	t.Setenv("MACHINE_ID", "m-1")
	n := countLots(t)
	s := newLotSession()
	for _, v := range []float64{0, 1, 1, 0, 1} {
		handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": v}), nil, config.AppConfig{}, nil)
	}
	if *n != 2 {
		t.Fatalf("lots = %d, want 2", *n)
	}
}

// Latched: speed > 200 during the run, meter > 1500, then the machine
// stops (speed 0) and X26 is pressed in a poll where the meter is already
// reset — still a lot, because both conditions held since the last lot.
func TestLotLatchedConditions(t *testing.T) {
	t.Setenv("MACHINE_ID", "m-1")
	t.Setenv("CASE_LOT_REQUIRE_D418", ">1500")
	t.Setenv("CASE_LOT_REQUIRE_d392", ">200")
	n := countLots(t)
	s := newLotSession()
	step := func(x26, meter, speed float64) {
		handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": x26, "d418": meter, "d392": speed}), nil, config.AppConfig{}, nil)
	}

	step(0, 0, 0)
	step(0, 300, 250) // running, speed condition met
	step(1, 300, 0)   // early reset press: meter never > 1500 → ignored
	step(0, 300, 0)
	if *n != 0 {
		t.Fatalf("early press created a lot")
	}
	step(0, 1800, 240) // meter condition met
	step(0, 1900, 0)   // stopped
	step(1, 0, 0)      // reset pressed, meter already cleared in this poll
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 after valid press", *n)
	}
	// Latches cleared: pressing again straight away is ignored.
	step(0, 0, 0)
	step(1, 0, 0)
	if *n != 1 {
		t.Fatalf("second press without a new run created a lot")
	}
}

// "now": every condition must hold in the trigger poll itself.
func TestLotNowMode(t *testing.T) {
	t.Setenv("MACHINE_ID", "m-1")
	t.Setenv("CASE_LOT_CONDITION_MODE", "now")
	t.Setenv("CASE_LOT_REQUIRE_d418", ">1500")
	t.Setenv("CASE_LOT_REQUIRE_m50", "=1")
	n := countLots(t)
	s := newLotSession()
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(0), "d418": float64(2000), "m50": float64(1)}), nil, config.AppConfig{}, nil)
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(1), "d418": float64(0), "m50": float64(1)}), nil, config.AppConfig{}, nil)
	if *n != 0 {
		t.Fatalf("now mode used a past value")
	}
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(0), "d418": float64(2000), "m50": float64(1)}), nil, config.AppConfig{}, nil)
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(1), "d418": "2000", "m50": true}), nil, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want 1", *n)
	}
}

// Any number of conditions; a register never received keeps it unmet;
// an invalid condition is skipped (not blocking).
func TestLotManyConditionsAndBadEntry(t *testing.T) {
	t.Setenv("MACHINE_ID", "m-1")
	t.Setenv("CASE_LOT_REQUIRE_d1", ">1")
	t.Setenv("CASE_LOT_REQUIRE_d2", ">2")
	t.Setenv("CASE_LOT_REQUIRE_d3", ">3")
	t.Setenv("CASE_LOT_REQUIRE_d4", ">4")
	t.Setenv("CASE_LOT_REQUIRE_d5", "oops")
	n := countLots(t)
	s := newLotSession()
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(0), "d1": float64(5), "d2": float64(5), "d3": float64(5)}), nil, config.AppConfig{}, nil)
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(1)}), nil, config.AppConfig{}, nil)
	if *n != 0 {
		t.Fatalf("d4 never received but a lot was created")
	}
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(0), "d4": float64(9)}), nil, config.AppConfig{}, nil)
	handleLotCase(s, lotTK, lotFrame(map[string]any{"x26": float64(1)}), nil, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 once all four held", *n)
	}
}
