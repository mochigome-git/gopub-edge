package handler

import (
	"testing"
	"time"
)

// operator over 3 registers; each register holds 2 chars stored reversed
// (reverseString undoes it), so "豊住" in w50 is sent as "住豊".
func withOperatorRegs(t *testing.T) {
	t.Setenv("CASE_JOB_STR_OPERATOR_RE0", "w50")
	t.Setenv("CASE_JOB_STR_OPERATOR_RE1", "w51")
	t.Setenv("CASE_JOB_STR_OPERATOR_RE2", "w52")
}

func frameWith(trigger float64, regs map[string]any) map[string]any {
	m := map[string]any{"m100": trigger, "w69": 20.0, "d500": 10.0}
	for k, v := range regs {
		m[k] = v
	}
	return m
}

var fullName = map[string]any{"w50": "住豊", "w51": "郎太", "w52": "  "}

func operatorOf(h *jobHarness) any {
	if len(h.posted) != 1 {
		h.t.Fatalf("posted %d jobs, want 1", len(h.posted))
	}
	return h.posted[0]["operator"]["operator"]
}

func endJob(h *jobHarness) {
	for i := 0; i < 4; i++ {
		h.frame(time.Second, frameWith(0, nil))
	}
}

func TestOperatorPartialBatchDoesNotOverwrite(t *testing.T) {
	h := newJobHarness(t)
	withOperatorRegs(t)
	h.frame(time.Second, frameWith(1, fullName))
	// split batch: only the last register arrived
	h.frame(time.Second, frameWith(1, map[string]any{"w51": "郎太"}))
	endJob(h)
	if got := operatorOf(h); got != "豊住太郎" {
		t.Fatalf("operator = %v, want 豊住太郎", got)
	}
}

func TestOperatorBlankAtEndKeepsName(t *testing.T) {
	h := newJobHarness(t)
	withOperatorRegs(t)
	h.frame(time.Second, frameWith(1, fullName))
	// HMI cleared the name in the last ON frame
	h.frame(time.Second, frameWith(1, map[string]any{"w50": "", "w51": "", "w52": ""}))
	endJob(h)
	if got := operatorOf(h); got != "豊住太郎" {
		t.Fatalf("operator = %v, want 豊住太郎", got)
	}
}

func TestOperatorEnteredBeforeStartIsKept(t *testing.T) {
	h := newJobHarness(t)
	withOperatorRegs(t)
	h.frame(time.Second, frameWith(0, fullName)) // typed while idle
	// START pressed — HMI clears the name for the whole job
	blank := map[string]any{"w50": "", "w51": "", "w52": ""}
	h.frame(time.Second, frameWith(1, blank))
	h.frame(time.Second, frameWith(1, blank))
	endJob(h)
	if got := operatorOf(h); got != "豊住太郎" {
		t.Fatalf("operator = %v, want 豊住太郎 (from before START)", got)
	}
}

func TestOperatorChangedDuringJobWins(t *testing.T) {
	h := newJobHarness(t)
	withOperatorRegs(t)
	h.frame(time.Second, frameWith(0, fullName))
	h.frame(time.Second, frameWith(1, map[string]any{"w50": "田山", "w51": "子花", "w52": ""}))
	endJob(h)
	if got := operatorOf(h); got != "山田花子" {
		t.Fatalf("operator = %v, want 山田花子", got)
	}
}

func TestPreStartNotCarriedIntoLaterJobs(t *testing.T) {
	h := newJobHarness(t)
	withOperatorRegs(t)
	h.frame(time.Second, frameWith(0, fullName))
	h.frame(time.Second, frameWith(1, nil))
	endJob(h)
	// second job: no name anywhere, registers absent while idle and running
	h.posted = nil
	h.frame(time.Second, frameWith(1, nil))
	endJob(h)
	if got := operatorOf(h); got != nil {
		t.Fatalf("operator = %v, want none (pre-start value must not carry to the next job)", got)
	}
}
