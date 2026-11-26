// mcs_run_conditions.go
package handler

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// Extra rules for when the mcsState case counts the machine as RUNNING.
// All optional; with none set the run signal (RUN_START) alone decides,
// exactly as before.
//
//	// every condition must hold (same syntax as CASE_LOT_REQUIRE_*)
//	CASE_MCS_STATE_RUN_REQUIRE_d392=>200      # line speed above 200
//
//	// at least N of these registers must be ON (value != 0)
//	CASE_MCS_STATE_RUN_COUNT=m28,m29,m30,m31,m32,m33
//	CASE_MCS_STATE_RUN_COUNT_MIN=4            # default: all of them
//
//	// debounce, seconds: how long the result must hold before the state
//	// changes, so a short speed dip doesn't log a stop/start pair
//	CASE_MCS_STATE_RUN_ON_DELAY_SEC=10
//	CASE_MCS_STATE_RUN_OFF_DELAY_SEC=30
//
// RUN_START becomes optional once any REQUIRE/COUNT rule is set; when it
// is set too, it is ANDed with the rules. Register names are
// case-insensitive. A register missing from a poll keeps its last value.
const (
	mcsRunRequirePrefix = "RUN_REQUIRE_"
	mcsRunCountKey      = "RUN_COUNT"
	mcsRunCountMinKey   = "RUN_COUNT_MIN"
	mcsRunOnDelayKey    = "RUN_ON_DELAY_SEC"
	mcsRunOffDelayKey   = "RUN_OFF_DELAY_SEC"
)

type mcsRunRules struct {
	require  []lotCondition
	count    []string // lower-case registers
	countMin int
	onDelay  time.Duration
	offDelay time.Duration
}

// hasRules reports whether any REQUIRE/COUNT rule is configured.
func (r mcsRunRules) hasRules() bool { return len(r.require) > 0 || len(r.count) > 0 }

// registers lists every register the rules read.
func (r mcsRunRules) registers() []string {
	regs := make([]string, 0, len(r.require)+len(r.count))
	for _, c := range r.require {
		regs = append(regs, c.Register)
	}
	return append(regs, r.count...)
}

// loadMCSRunRules reads the RUN_* rules from the CASE_MCS_STATE_* map.
func loadMCSRunRules(tr map[string]string) mcsRunRules {
	var r mcsRunRules
	for name, val := range tr {
		if !strings.HasPrefix(name, mcsRunRequirePrefix) {
			continue
		}
		reg := strings.TrimPrefix(name, mcsRunRequirePrefix)
		if strings.TrimSpace(reg) == "" {
			continue
		}
		c, err := parseLotCondition(reg, val)
		if err != nil {
			warnLotOnce("handleMCSStateCase: ignoring run condition — " +
				strings.Replace(err.Error(), lotRequirePrefix, mcsStateFieldPrefix+mcsRunRequirePrefix, 1))
			continue
		}
		r.require = append(r.require, c)
	}
	sort.Slice(r.require, func(i, j int) bool { return r.require[i].Register < r.require[j].Register })

	for _, reg := range strings.Split(tr[mcsRunCountKey], ",") {
		reg = strings.ToLower(strings.TrimSpace(strings.Trim(reg, `"'`)))
		if reg != "" {
			r.count = append(r.count, reg)
		}
	}
	r.countMin = len(r.count)
	if s := strings.TrimSpace(tr[mcsRunCountMinKey]); s != "" && len(r.count) > 0 {
		if n, err := strconv.Atoi(s); err == nil && n >= 0 {
			r.countMin = min(n, len(r.count))
		} else {
			warnLotOnce(fmt.Sprintf("handleMCSStateCase: %s%s=%q is not a whole number — requiring all %d",
				mcsStateFieldPrefix, mcsRunCountMinKey, s, len(r.count)))
		}
	}
	r.onDelay = parseDelaySec(tr, mcsRunOnDelayKey)
	r.offDelay = parseDelaySec(tr, mcsRunOffDelayKey)
	return r
}

func parseDelaySec(tr map[string]string, key string) time.Duration {
	s := strings.TrimSpace(tr[key])
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		warnLotOnce(fmt.Sprintf("handleMCSStateCase: %s%s=%q is not a number of seconds — using 0",
			mcsStateFieldPrefix, key, s))
		return 0
	}
	return time.Duration(f * float64(time.Second))
}

// updateRunRuleValues stores this poll's value of every rule register.
// Returns true once every register has been received at least once.
func updateRunRuleValues(sess *session.Session, r mcsRunRules, jsonPayloads *utils.SafeJsonPayloads) bool {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	if sess.RunRuleLast == nil {
		sess.RunRuleLast = make(map[string]float64)
	}
	all := true
	for _, reg := range r.registers() {
		if raw, ok := jsonPayloads.Get(reg); ok {
			if v, num := readNumber(raw); num {
				sess.RunRuleLast[reg] = v
			} else {
				warnLotOnceKey("state-nonnum:"+reg, fmt.Sprintf("handleMCSStateCase: register %q has non-numeric value %v (%T)", reg, raw, raw))
			}
		}
		if _, seen := sess.RunRuleLast[reg]; !seen {
			all = false
		}
	}
	return all
}

// evalRunRules checks the rules against the last known values and returns
// a short description for logs, e.g. "d392 > 200 ok (300), count 5/4 ok".
func evalRunRules(sess *session.Session, r mcsRunRules) (bool, string) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	ok := true
	parts := make([]string, 0, len(r.require)+1)
	for _, c := range r.require {
		v, seen := sess.RunRuleLast[c.Register]
		pass := seen && c.eval(v)
		ok = ok && pass
		parts = append(parts, fmt.Sprintf("%s %s (%s)", c, okText(pass), fmtNum(v, seen)))
	}
	if len(r.count) > 0 {
		on := 0
		for _, reg := range r.count {
			if sess.RunRuleLast[reg] != 0 {
				on++
			}
		}
		pass := on >= r.countMin
		ok = ok && pass
		parts = append(parts, fmt.Sprintf("count %d/%d %s", on, r.countMin, okText(pass)))
	}
	return ok, strings.Join(parts, ", ")
}

// debounceRun turns the raw run condition into the debounced RunActive.
// The first evaluation after start is taken as-is (no delay), so a restart
// while the machine runs doesn't log a false idle period.
func debounceRun(sess *session.Session, raw bool, r mcsRunRules, now time.Time) bool {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	if !sess.RunInit {
		sess.RunInit = true
		sess.RunActive = raw
		sess.RunRawSince = now
		sess.RunRawLast = raw
		return raw
	}
	if raw != sess.RunRawLast {
		sess.RunRawLast = raw
		sess.RunRawSince = now
	}
	delay := r.offDelay
	if raw {
		delay = r.onDelay
	}
	if raw != sess.RunActive && now.Sub(sess.RunRawSince) >= delay {
		sess.RunActive = raw
	}
	return sess.RunActive
}

func okText(b bool) string {
	if b {
		return "ok"
	}
	return "NOT met"
}

func fmtNum(v float64, seen bool) string {
	if !seen {
		return "not received"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// logRunChange logs the rule summary when the raw run condition flips.
func logRunChange(sess *session.Session, raw bool, summary string) {
	sess.Mutex.Lock()
	changed := !sess.RunLogged || sess.RunLoggedVal != raw
	sess.RunLogged, sess.RunLoggedVal = true, raw
	sess.Mutex.Unlock()
	if changed {
		log.Printf("handleMCSStateCase: run condition %s — %s", map[bool]string{true: "MET", false: "not met"}[raw], summary)
	}
}
