package handler

import (
	"testing"
	"time"

	"gopub-edge/internal/utils"
)

func runRulesFromEnv(t *testing.T, env map[string]string) mcsRunRules {
	t.Helper()
	for k, v := range env {
		t.Setenv(mcsStateFieldPrefix+k, v)
	}
	return loadMCSRunRules(utils.GetKeyTransformationsFromEnv(mcsStateFieldPrefix))
}

func TestLoadMCSRunRules(t *testing.T) {
	r := runRulesFromEnv(t, map[string]string{
		"RUN_REQUIRE_D392":  ">200",
		"RUN_COUNT":         "M28, m29,m30 ,m31,m32,m33",
		"RUN_COUNT_MIN":     "4",
		"RUN_ON_DELAY_SEC":  "10",
		"RUN_OFF_DELAY_SEC": "30",
	})
	if len(r.require) != 1 || r.require[0].Register != "d392" || r.require[0].Op != ">" || r.require[0].Value != 200 {
		t.Fatalf("require = %+v", r.require)
	}
	if len(r.count) != 6 || r.count[0] != "m28" || r.count[2] != "m30" || r.countMin != 4 {
		t.Fatalf("count = %v min %d", r.count, r.countMin)
	}
	if r.onDelay != 10*time.Second || r.offDelay != 30*time.Second {
		t.Fatalf("delays = %v / %v", r.onDelay, r.offDelay)
	}
}

func TestRunCountMinDefaultsToAll(t *testing.T) {
	r := runRulesFromEnv(t, map[string]string{"RUN_COUNT": "m1,m2,m3"})
	if r.countMin != 3 {
		t.Fatalf("countMin = %d, want 3", r.countMin)
	}
}

func TestEvalRunRules(t *testing.T) {
	r := runRulesFromEnv(t, map[string]string{
		"RUN_REQUIRE_d392": ">200",
		"RUN_COUNT":        "m28,m29,m30,m31,m32",
		"RUN_COUNT_MIN":    "4",
	})
	sess := newLotSession()
	frame := func(speed any, dryersOn int) {
		vals := map[string]any{"d392": speed}
		for i, reg := range r.count {
			vals[reg] = float64(map[bool]int{true: 1, false: 0}[i < dryersOn])
		}
		updateRunRuleValues(sess, r, lotFrame(vals))
	}

	frame("300.000", 4) // speed arrives as a string from msp-mcs
	if ok, s := evalRunRules(sess, r); !ok {
		t.Fatalf("speed 300, 4 dryers: want met, got %s", s)
	}
	frame("300.000", 3)
	if ok, _ := evalRunRules(sess, r); ok {
		t.Fatal("3 dryers: want not met")
	}
	frame(150.0, 5)
	if ok, _ := evalRunRules(sess, r); ok {
		t.Fatal("speed 150: want not met")
	}
	// a partial poll (speed only) keeps the dryer values
	updateRunRuleValues(sess, r, lotFrame(map[string]any{"d392": 250.0}))
	if ok, s := evalRunRules(sess, r); !ok {
		t.Fatalf("partial poll: want met, got %s", s)
	}
}

func TestUpdateRunRuleValuesAllSeen(t *testing.T) {
	r := runRulesFromEnv(t, map[string]string{"RUN_REQUIRE_d392": ">200", "RUN_COUNT": "m1,m2"})
	sess := newLotSession()
	if updateRunRuleValues(sess, r, lotFrame(map[string]any{"d392": 1.0, "m1": 1.0})) {
		t.Fatal("m2 never received: want allSeen=false")
	}
	if !updateRunRuleValues(sess, r, lotFrame(map[string]any{"m2": 0.0})) {
		t.Fatal("all received over two polls: want allSeen=true")
	}
}

func TestDebounceRun(t *testing.T) {
	r := mcsRunRules{onDelay: 10 * time.Second, offDelay: 30 * time.Second}
	sess := newLotSession()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

	steps := []struct {
		sec  int
		raw  bool
		want bool
	}{
		{0, false, false}, // first poll seeds with no delay
		{1, true, false},  // rising — wait 10 s
		{10, true, false},
		{11, true, true}, // held 10 s → running
		{20, false, true},
		{25, true, true},  // 5 s dip, never reached 30 s → stays running
		{30, false, true}, // falling again
		{59, false, true},
		{60, false, false}, // held 30 s → idle
	}
	for _, s := range steps {
		if got := debounceRun(sess, s.raw, r, at(s.sec)); got != s.want {
			t.Fatalf("t=%ds raw=%v: running=%v, want %v", s.sec, s.raw, got, s.want)
		}
	}
}

func TestDebounceSeedsRunningAfterRestart(t *testing.T) {
	r := mcsRunRules{onDelay: 10 * time.Second}
	sess := newLotSession()
	if !debounceRun(sess, true, r, time.Now()) {
		t.Fatal("first poll while running: want running immediately (no false idle on restart)")
	}
}

func TestResolveTargetPriority(t *testing.T) {
	sess := newLotSession()
	if got := resolveTarget(sess, false, true).state; got != mcsStateRunning {
		t.Fatalf("running: got %s", got)
	}
	sess.DowntimeRequested = true
	if got := resolveTarget(sess, false, true).state; got != mcsStateDowntime {
		t.Fatalf("downtime button while running: got %s", got)
	}
	sess.DowntimeRequested = false
	if got := resolveTarget(sess, false, false).state; got != mcsStateIdle {
		t.Fatalf("idle: got %s", got)
	}
}
