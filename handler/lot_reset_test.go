package handler

import (
	"testing"

	"gopub-edge/config"
	"gopub-edge/model"
)

func meterBatch(x17 float64, meters ...any) ([]model.Message, map[string]any) {
	var msgs []model.Message
	for _, m := range meters {
		msgs = append(msgs, model.Message{Address: "X17", Value: x17}, model.Message{Address: "D676", Value: m})
	}
	return msgs, map[string]any{"x17": x17, "d676": meters[len(meters)-1]}
}

func TestLotFromMeterReset(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	t.Setenv(lotResetRegisterEnv, "d676")
	n := countLots(t)
	s := newLotSession()
	step := func(meters ...any) {
		msgs, merged := meterBatch(0, meters...)
		handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	}

	step("8120.50", "8125.10") // first readings only set the baseline
	if *n != 0 {
		t.Fatalf("lots = %d after baseline, want 0", *n)
	}
	step("8127.14", "0.00000") // button never seen, but the meter reset
	if *n != 1 {
		t.Fatalf("lots = %d after meter reset, want 1", *n)
	}
	step("0.5", "3.2", "40") // counting up again — no lot
	if *n != 1 {
		t.Fatalf("lots = %d while counting up, want 1", *n)
	}
}

func TestLotSmallDropIsNotAReset(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	t.Setenv(lotResetRegisterEnv, "d676")
	n := countLots(t)
	s := newLotSession()
	msgs, merged := meterBatch(0, 500.0, 450.0) // 50 < default 100
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	if *n != 0 {
		t.Fatalf("lots = %d, want 0 for a 50 m drop", *n)
	}
}

func TestLotButtonAndMeterResetMakeOneLot(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	t.Setenv(lotResetRegisterEnv, "d676")
	n := countLots(t)
	s := newLotSession()

	msgs, merged := meterBatch(0, 3000.0)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	// press caught in this batch...
	msgs, merged = meterBatch(1, 3001.0)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	// ...and the meter reset arrives in the next one
	msgs, merged = meterBatch(0, 0.0)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 (cooldown stops the second trigger)", *n)
	}
}

func TestRunLength(t *testing.T) {
	if got := runLength([]bool{false, false, false, true, true, false}); got != "0×3 1×2 0×1" {
		t.Fatalf("runLength = %q", got)
	}
}

func TestLotDebugDoesNotPanic(t *testing.T) {
	t.Setenv("LOT_DEBUG", "true")
	t.Setenv("MACHINE_ID", "m1")
	t.Setenv(lotResetRegisterEnv, "d676")
	t.Setenv(lotRequirePrefix+"d392", ">200")
	countLots(t)
	s := newLotSession()
	handleLotCase(s, x17TK, lotFrame(map[string]any{"d392": 1.0}), []model.Message{{Address: "D392", Value: 1.0}}, config.AppConfig{}, nil)
	msgs, merged := meterBatch(1, 500.0, 0.0)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
}
