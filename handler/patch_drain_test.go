package handler

import (
	"testing"

	"gopub-edge/config"
	"gopub-edge/internal/session"
)

func queued(n int) chan string {
	ch := make(chan string, 10)
	for i := 0; i < n; i++ {
		ch <- `[{"address":"X17","value":1}]`
	}
	return ch
}

// patch.Pub is nil in tests, so the publish itself just logs an error;
// what matters is what happens to the input queue afterwards.
func TestJobAndLotPostsKeepTheInputQueue(t *testing.T) {
	for _, kind := range []envelopeKind{envelopeJob, envelopeLot} {
		s := session.NewSession()
		s.ProcessedPayloadsMap["machine_id"] = map[string]any{"machine_id": "m1"}
		ch := queued(3)
		processPatchFrom(s, s.ProcessedPayloadsMap, []string{"machine_id"}, config.AppConfig{}, nil, ch, nil, kind)
		if len(ch) != 3 {
			t.Fatalf("%s post left %d queued batches, want 3 (queue must not be drained)", kind, len(ch))
		}
	}
}

func TestStatePostKeepsTheInputQueue(t *testing.T) {
	s := session.NewSession()
	s.ProcessedPayloadsMap["machine_state"] = map[string]any{"machine_state": "running"}
	ch := queued(2)
	processPatchKeepQueue(s, []string{"machine_state"}, config.AppConfig{}, ch)
	if len(ch) != 2 {
		t.Fatalf("state post left %d queued batches, want 2", len(ch))
	}
}

func TestLegacyReadingsPostStillDrains(t *testing.T) {
	s := session.NewSession()
	s.ProcessedPayloadsMap["ch1"] = map[string]any{"w": 1.0}
	ch := queued(2)
	processPatch(s, []string{"ch1"}, config.AppConfig{}, nil, ch, nil)
	if len(ch) != 0 {
		t.Fatalf("legacy readings post left %d queued, want 0 (unchanged behaviour)", len(ch))
	}
}

func TestJobWithManyNullsIsStillPosted(t *testing.T) {
	s := session.NewSession()
	frozen := map[string]map[string]any{
		"a": {"a": nil}, "b": {"b": nil}, "c": {"c": nil}, "d": {"d": nil},
		"total_output": {"total_output": 10.0},
	}
	ch := queued(1)
	// would previously abort with "more than 3 null values"; now it reaches
	// the publish step (which logs an error because patch.Pub is nil)
	processPatchFrom(s, frozen, []string{"a", "b", "c", "d", "total_output"}, config.AppConfig{}, nil, ch, nil, envelopeJob)
	if _, still := frozen["total_output"]; still {
		t.Fatal("job keys not consumed — the post was aborted")
	}
}
