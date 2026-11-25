// lot_conditions.go
package handler

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// Conditions that must hold before the lot trigger may create a new lot.
// Any number of them, one env var each:
//
//	CASE_LOT_REQUIRE_<register>=<condition>
//
//	CASE_LOT_REQUIRE_d418=>1500    # meter above 1500
//	CASE_LOT_REQUIRE_d392=>200     # speed above 200
//	CASE_LOT_REQUIRE_m50==1        # a bit that must be ON
//	CASE_LOT_REQUIRE_d500=<=30     # or below / equal / not equal
//
// <condition> is an optional operator — >, >=, <, <=, =, ==, != — followed
// by a number. A bare number means ">" (e.g. CASE_LOT_REQUIRE_d418=1500).
// Register names are case-insensitive (D418 = d418).
//
// When conditions are checked (CASE_LOT_CONDITION_MODE):
//
//	latched (default)  each condition only has to be true at SOME poll since
//	                   the last lot was created — "the meter went over 1500
//	                   and the speed went over 200 during this lot". Needed
//	                   when the trigger is a reset button pressed after the
//	                   machine has stopped (speed already 0, meter maybe
//	                   already cleared in that same poll).
//	now                every condition must be true in the poll where the
//	                   trigger turns ON.
//
// A trigger press while a condition is unmet is ignored (logged) and the
// current lot continues; latched conditions are kept, not cleared.
// With no CASE_LOT_REQUIRE_* set the trigger behaves as before: every
// OFF->ON edge creates a lot.
const (
	lotRequirePrefix = "CASE_LOT_REQUIRE_"
	lotModeEnv       = "CASE_LOT_CONDITION_MODE"

	lotModeLatched = "latched"
	lotModeNow     = "now"
)

type lotCondition struct {
	Register string  // lower-case register key, as in the payload map
	Op       string  // >, >=, <, <=, ==, !=
	Value    float64 // threshold
	Raw      string  // original text, for logs
}

func (c lotCondition) String() string {
	return fmt.Sprintf("%s %s %s", c.Register, c.Op, strconv.FormatFloat(c.Value, 'f', -1, 64))
}

func (c lotCondition) eval(v float64) bool {
	switch c.Op {
	case ">":
		return v > c.Value
	case ">=":
		return v >= c.Value
	case "<":
		return v < c.Value
	case "<=":
		return v <= c.Value
	case "==":
		return v == c.Value
	case "!=":
		return v != c.Value
	}
	return false
}

// parseLotCondition parses ">1500", ">= 200", "==1", "1500" (= ">1500").
func parseLotCondition(register, raw string) (lotCondition, error) {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, `"'`)
	s = strings.TrimSpace(s)
	op := ">"
	for _, cand := range []string{">=", "<=", "==", "!=", ">", "<", "="} {
		if strings.HasPrefix(s, cand) {
			op = cand
			s = strings.TrimSpace(strings.TrimPrefix(s, cand))
			break
		}
	}
	if op == "=" {
		op = "=="
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return lotCondition{}, fmt.Errorf("%s%s=%q: want an optional >, >=, <, <=, =, != followed by a number", lotRequirePrefix, register, raw)
	}
	return lotCondition{Register: strings.ToLower(register), Op: op, Value: v, Raw: raw}, nil
}

// Each bad env value is reported once, not on every poll.
var lotWarned sync.Map

func warnLotOnce(msg string) {
	if _, seen := lotWarned.LoadOrStore(msg, true); !seen {
		log.Print(msg)
	}
}

// loadLotConditions reads every CASE_LOT_REQUIRE_* from the environment,
// sorted by register so logs are stable.
func loadLotConditions() []lotCondition {
	raw := utils.GetKeyTransformationsFromEnv(lotRequirePrefix)
	conds := make([]lotCondition, 0, len(raw))
	for reg, val := range raw {
		if strings.TrimSpace(reg) == "" {
			continue
		}
		c, err := parseLotCondition(reg, val)
		if err != nil {
			warnLotOnce("handleLotCase: ignoring condition — " + err.Error())
			continue
		}
		conds = append(conds, c)
	}
	sort.Slice(conds, func(i, j int) bool { return conds[i].Register < conds[j].Register })
	return conds
}

func lotConditionMode() string {
	m := strings.ToLower(strings.TrimSpace(os.Getenv(lotModeEnv)))
	switch m {
	case "", lotModeLatched:
		return lotModeLatched
	case lotModeNow:
		return lotModeNow
	default:
		warnLotOnce(fmt.Sprintf("handleLotCase: %s=%q is not latched|now — using latched", lotModeEnv, m))
		return lotModeLatched
	}
}

// readNumber coerces a payload value (float64, numeric string, bool).
func readNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

// updateLotConditions records this poll's values: the last value seen per
// register (for logs) and, per condition, whether it has been true since
// the last lot. Caller holds no lock.
func updateLotConditions(sess *session.Session, conds []lotCondition, jsonPayloads *utils.SafeJsonPayloads) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	if sess.LotCondMet == nil {
		sess.LotCondMet = make(map[string]bool)
	}
	if sess.LotCondLast == nil {
		sess.LotCondLast = make(map[string]float64)
	}
	for _, c := range conds {
		raw, ok := jsonPayloads.Get(c.Register)
		if !ok {
			continue
		}
		v, ok := readNumber(raw)
		if !ok {
			warnLotOnce(fmt.Sprintf("handleLotCase: register %q has non-numeric value %v (%T)", c.Register, raw, raw))
			continue
		}
		sess.LotCondLast[c.Register] = v
		if c.eval(v) {
			sess.LotCondMet[c.String()] = true
		}
	}
}

// lotConditionsSatisfied reports whether a trigger edge may create a lot,
// and a human-readable list of the unmet conditions when it may not.
func lotConditionsSatisfied(
	sess *session.Session,
	conds []lotCondition,
	mode string,
	jsonPayloads *utils.SafeJsonPayloads,
) (bool, []string) {
	var unmet []string
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	for _, c := range conds {
		var ok bool
		if mode == lotModeNow {
			if raw, present := jsonPayloads.Get(c.Register); present {
				if v, num := readNumber(raw); num {
					ok = c.eval(v)
				}
			}
		} else {
			ok = sess.LotCondMet[c.String()]
		}
		if !ok {
			last, seen := sess.LotCondLast[c.Register]
			if seen {
				unmet = append(unmet, fmt.Sprintf("%s (last %s)", c, strconv.FormatFloat(last, 'f', -1, 64)))
			} else {
				unmet = append(unmet, fmt.Sprintf("%s (not received)", c))
			}
		}
	}
	return len(unmet) == 0, unmet
}

// resetLotConditions clears the latches once a lot has been created.
func resetLotConditions(sess *session.Session) {
	sess.Mutex.Lock()
	sess.LotCondMet = make(map[string]bool)
	sess.Mutex.Unlock()
}
