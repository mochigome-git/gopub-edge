package utils

import (
	"testing"

	"gopub-edge/internal/session"
)

func cycle(s *session.Session, remark, reason float64) {
	p := NewSafeJsonPayloads()
	p.Set("d26", remark)
	p.Set("d57", reason)
	RemarkMapping(p, s)
	if _, ok := p.Get("d57"); ok {
		panic("d57 not removed from payload")
	}
}

func held(s *session.Session, key string) (any, bool) {
	m, ok := s.ProcessedPayloadsMap[key]
	if !ok {
		return nil, false
	}
	return m[key], true
}

func TestReasonLatchedWithRemark(t *testing.T) {
	s := session.NewSession()

	// Good part: no reason sent even if the register has a stale value.
	cycle(s, 0, 2)
	if _, ok := held(s, "ch1_reason"); ok {
		t.Fatal("reason sent with a good remark")
	}

	// Reject with reason 2.
	cycle(s, 5, 2)
	if v, _ := held(s, "ch1_reason"); v != 2 {
		t.Fatalf("reason = %v, want 2", v)
	}

	// PLC resets the reason register while the reject is still latched.
	cycle(s, 5, 0)
	if v, _ := held(s, "ch1_reason"); v != 2 {
		t.Fatalf("reason dropped while reject held: %v", v)
	}

	// Remark goes back to 0: reason stays until the remark clears.
	for i := 0; i < NormalConfirmCycles-1; i++ {
		cycle(s, 0, 0)
		if v, _ := held(s, "ch1_reason"); v != 2 {
			t.Fatalf("cycle %d: reason dropped before remark cleared: %v", i, v)
		}
	}
	cycle(s, 0, 0)
	if _, ok := held(s, "ch1_reason"); ok {
		t.Fatal("reason kept after remark cleared")
	}
}
