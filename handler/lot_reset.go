// lot_reset.go
package handler

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

// A second way to start a lot that doesn't depend on catching a short
// button press: watch a counter the button resets (e.g. the winding meter).
// The press may last < 1 s, but the meter stays reset, so a slow reader
// still sees it.
//
//	CASE_LOT_RESET_REGISTER=d676     # counter that drops back when a lot starts
//	CASE_LOT_RESET_MIN_DROP=100      # drop between two readings that counts as a reset (default 100)
//	CASE_LOT_COOLDOWN_SEC=30         # ignore further triggers for N s after a lot
//	                                 # (default 30 with RESET_REGISTER set, else 0)
//
// Works alongside the TRIGGER_DEVICE button edge: whichever is seen first
// creates the lot, and the cooldown stops the other from creating a second
// one. CASE_LOT_REQUIRE_* conditions apply to both.
const (
	lotResetRegisterEnv = "CASE_LOT_RESET_REGISTER"
	lotResetMinDropEnv  = "CASE_LOT_RESET_MIN_DROP"
	lotCooldownEnv      = "CASE_LOT_COOLDOWN_SEC"

	defaultLotResetMinDrop = 100.0
	defaultLotCooldown     = 30 * time.Second
)

func envFloat(name string, def float64) float64 {
	s := strings.TrimSpace(os.Getenv(name))
	if s == "" {
		return def
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		warnLotOnce(fmt.Sprintf("handleLotCase: %s=%q is not a number — using %v", name, s, def))
		return def
	}
	return f
}

// lotCooldown defaults to 30 s only when the reset register is configured
// (that is when one press arrives twice: button edge + meter drop); without
// it the default is 0, so the button behaves exactly as before.
func lotCooldown() time.Duration {
	def := 0.0
	if strings.TrimSpace(os.Getenv(lotResetRegisterEnv)) != "" {
		def = defaultLotCooldown.Seconds()
	}
	return time.Duration(envFloat(lotCooldownEnv, def) * float64(time.Second))
}

// lotResetEdge walks every reading of the reset register in this batch and
// reports whether it dropped by at least the minimum since the reading
// before (e.g. "d676 8127 -> 0"). The first reading after start only sets
// the baseline.
func lotResetEdge(sess *session.Session, messages []model.Message, jsonPayloads *utils.SafeJsonPayloads) (bool, string) {
	reg := strings.ToLower(strings.TrimSpace(os.Getenv(lotResetRegisterEnv)))
	if reg == "" {
		return false, ""
	}
	minDrop := envFloat(lotResetMinDropEnv, defaultLotResetMinDrop)

	var vals []float64
	for _, m := range messages {
		if strings.EqualFold(m.Address, reg) {
			if v, ok := readNumber(m.Value); ok {
				vals = append(vals, v)
			}
		}
	}
	if len(vals) == 0 {
		if raw, ok := jsonPayloads.Get(reg); ok {
			if v, ok := readNumber(raw); ok {
				vals = append(vals, v)
			}
		}
	}

	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	edge, detail := false, ""
	for _, v := range vals {
		if sess.LotResetHave && sess.LotResetLast-v >= minDrop {
			edge = true
			detail = fmt.Sprintf("%s reset %s -> %s", reg,
				strconv.FormatFloat(sess.LotResetLast, 'f', -1, 64), strconv.FormatFloat(v, 'f', -1, 64))
		}
		sess.LotResetLast, sess.LotResetHave = v, true
	}
	return edge, detail
}
