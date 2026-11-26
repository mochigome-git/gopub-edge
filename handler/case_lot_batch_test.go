package handler

import (
	"testing"

	"gopub-edge/config"
	"gopub-edge/model"
)

// batch builds one flusher batch: several polls of x17 (oldest first) with
// d418/d392 alongside, plus the merged payload the loader would build
// (last value wins).
func lotBatch(x17 []float64, d418, d392 float64) ([]model.Message, map[string]any) {
	var msgs []model.Message
	for _, v := range x17 {
		msgs = append(msgs,
			model.Message{Address: "X17", Value: v},
			model.Message{Address: "D418", Value: d418},
			model.Message{Address: "D392", Value: d392},
		)
	}
	return msgs, map[string]any{"x17": x17[len(x17)-1], "d418": d418, "d392": d392}
}

var x17TK = lotTK

func init() { x17TK.TriggerKey = "x17" }

func TestLotPressInsideOneBatchIsCaught(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	n := countLots(t)
	s := newLotSession()

	// press lasts 3 polls in the middle of a batch; the batch ends with 0
	msgs, merged := lotBatch([]float64{0, 0, 1, 1, 1, 0, 0}, 2100, 300)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 (press inside the batch)", *n)
	}

	// next batch all 0 — no new lot
	msgs, merged = lotBatch([]float64{0, 0, 0}, 2100, 300)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want still 1", *n)
	}
}

func TestLotHeldAcrossBatchesFiresOnce(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	n := countLots(t)
	s := newLotSession()
	for _, polls := range [][]float64{{0, 0, 1}, {1, 1, 1}, {1, 0, 0}} {
		msgs, merged := lotBatch(polls, 2100, 300)
		handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	}
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 for one long press", *n)
	}
}

func TestLotLatchesSpeedSeenMidBatch(t *testing.T) {
	t.Setenv("MACHINE_ID", "m1")
	t.Setenv(lotRequirePrefix+"d392", ">200")
	n := countLots(t)
	s := newLotSession()

	// speed 300 only in the middle of the batch, 0 at its end
	msgs := []model.Message{
		{Address: "D392", Value: "0.000"}, {Address: "X17", Value: 0.0},
		{Address: "D392", Value: "300.000"}, {Address: "X17", Value: 0.0},
		{Address: "D392", Value: "0.000"}, {Address: "X17", Value: 0.0},
	}
	handleLotCase(s, x17TK, lotFrame(map[string]any{"x17": 0.0, "d392": "0.000"}), msgs, config.AppConfig{}, nil)

	msgs, merged := lotBatch([]float64{0, 1, 0}, 200, 0)
	handleLotCase(s, x17TK, lotFrame(merged), msgs, config.AppConfig{}, nil)
	if *n != 1 {
		t.Fatalf("lots = %d, want 1 (speed > 200 was seen mid-batch)", *n)
	}
}
