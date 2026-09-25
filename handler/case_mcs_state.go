// case_mcs_state.go
package handler

import (
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

// mcsStateFieldPrefix drives the machine state machine. Register map:
//
//	// run signal — two momentary buttons (edge) ...
//	CASE_MCS_STATE_RUN_START=m300
//	CASE_MCS_STATE_RUN_STOP=m301
//	// ... or one level bit (point both at the SAME register, or omit RUN_STOP)
//	CASE_MCS_STATE_RUN_START=m300
//
//	// optional operator downtime button — same edge/level rule
//	CASE_MCS_STATE_DOWNTIME_START=m302
//	CASE_MCS_STATE_DOWNTIME_STOP=m303
//
//	// optional fault bits — any bit ON puts the machine in downtime with
//	// reason "fault"; the part after ALARM_ is the alarm code sent in
//	// alarms[] (lower-cased)
//	CASE_MCS_STATE_ALARM_ethercat_link_lost=m310
//	CASE_MCS_STATE_ALARM_servo_overload=m311
//
// Every poll the handler works out ONE target state from the inputs
// (fault > downtime button > run > idle) and, if it differs from the
// current state, publishes ONE transition message shaped for
// analytics.fn_track_machine_state_event:
//
//	{ "machine_state": "running", "machine_start": "...",
//	  "prev_state": "downtime", "prev_duration_sec": 300 }
//	{ "machine_state": "idle", "machine_stop": "...",
//	  "run_duration_sec": 39, "prev_state": "running", "prev_duration_sec": 39 }
//	{ "machine_state": "downtime", "machine_stop": "...", "run_duration_sec": 39,
//	  "reason": "fault", "alarms": ["ethercat_link_lost"],
//	  "prev_state": "running", "prev_duration_sec": 39 }
//	{ "machine_state": "downtime", "state_changed_at": "...",
//	  "reason": "button", "prev_state": "idle", "prev_duration_sec": 120 }
//
// A fault while running goes straight to downtime in one message (no
// idle-then-downtime pair), so the DB never has to reclassify.
// After a gopub-edge restart the first transition carries no prev_*
// fields, so the DB doesn't try to repair a gap that didn't happen.
const mcsStateFieldPrefix = "CASE_MCS_STATE_"

const mcsAlarmPrefix = "ALARM_"

const (
	mcsStateRunning  = "running"
	mcsStateIdle     = "idle"
	mcsStateDowntime = "downtime"

	mcsReasonFault  = "fault"
	mcsReasonButton = "button"
)

// UTC, millisecond precision — same shape the dashboard mocks send.
const mcsTimeLayout = "2006-01-02T15:04:05.000Z"

type mcsTarget struct {
	state  string
	reason string
	alarms []string
}

func handleMCSStateCase(
	sess *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	tr := utils.GetKeyTransformationsFromEnv(mcsStateFieldPrefix)

	runStartKey, hasRunStart := tr["RUN_START"]
	if !hasRunStart {
		log.Printf("handleMCSStateCase: no RUN_START register configured (CASE_MCS_STATE_RUN_START) — skipping")
		return
	}
	runStopKey, hasRunStop := tr["RUN_STOP"]
	downStartKey, hasDownStart := tr["DOWNTIME_START"]
	downStopKey, hasDownStop := tr["DOWNTIME_STOP"]

	runEdge := hasRunStop && runStopKey != runStartKey

	// 1. Update the inputs from this poll.
	if runEdge {
		updateRunEdge(sess, jsonPayloads, runStartKey, runStopKey)
	} else {
		updateRunLevel(sess, jsonPayloads, runStartKey)
	}
	if hasDownStart {
		if hasDownStop && downStopKey != downStartKey {
			updateDowntimeEdge(sess, jsonPayloads, downStartKey, downStopKey)
		} else {
			updateDowntimeLevel(sess, jsonPayloads, downStartKey)
		}
	}
	updateAlarms(sess, jsonPayloads, tr)

	// 2. Until the run signal has been seen at least once we don't know
	//    whether the machine is running, so don't report anything.
	sess.Mutex.Lock()
	known := sess.StateKnown
	sess.Mutex.Unlock()
	if !known {
		return
	}

	// 3. One target state, one transition.
	target := resolveTarget(sess, runEdge)
	keys := applyTransition(sess, target, time.Now().UTC())
	if len(keys) == 0 {
		return
	}

	// Readings envelope: every key here is in statusKeys (handler_shape.go),
	// so they land in the status jsonb the trigger reads.
	processPatch(sess, keys, cfg, nil, rMsgJSONChan, nil)
}

// ── Inputs ────────────────────────────────────────────────────────────────

// readBoolField mirrors the button-value coercion job_case.go uses.
func readBoolField(jsonPayloads *utils.SafeJsonPayloads, key string) (bool, bool) {
	raw, exists := jsonPayloads.Get(key)
	if !exists {
		return false, false
	}
	switch v := raw.(type) {
	case float64:
		return v == 1, true
	case string:
		return v == "1", true
	case bool:
		return v, true
	default:
		log.Printf("handleMCSStateCase: register %q has unexpected type %T (value %v) — ignoring", key, raw, raw)
		return false, false
	}
}

// updateRunLevel: one bit, 1 = running.
func updateRunLevel(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, key string) {
	on, ok := readBoolField(jsonPayloads, key)
	if !ok {
		return
	}
	sess.Mutex.Lock()
	sess.RunRequested = on
	sess.RunStartWasOn = on
	sess.StateKnown = true
	sess.Mutex.Unlock()
}

// updateRunEdge: separate momentary START / STOP buttons. If both fire in
// the same poll, STOP wins.
func updateRunEdge(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, startKey, stopKey string) {
	startOn, startOK := readBoolField(jsonPayloads, startKey)
	stopOn, stopOK := readBoolField(jsonPayloads, stopKey)

	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()

	if startOK {
		if startOn && !sess.RunStartWasOn {
			sess.RunRequested = true
			sess.StateKnown = true
		}
		sess.RunStartWasOn = startOn
	}
	if stopOK {
		if stopOn && !sess.RunStopWasOn {
			sess.RunRequested = false
			sess.StateKnown = true
		}
		sess.RunStopWasOn = stopOn
	}
}

// updateDowntimeLevel: one bit, 1 = operator downtime.
func updateDowntimeLevel(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, key string) {
	on, ok := readBoolField(jsonPayloads, key)
	if !ok {
		return
	}
	sess.Mutex.Lock()
	sess.DowntimeRequested = on
	sess.DowntimeStartWasOn = on
	sess.Mutex.Unlock()
}

// updateDowntimeEdge: separate momentary DOWNTIME_START / DOWNTIME_STOP.
func updateDowntimeEdge(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, startKey, stopKey string) {
	startOn, startOK := readBoolField(jsonPayloads, startKey)
	stopOn, stopOK := readBoolField(jsonPayloads, stopKey)

	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()

	if startOK {
		if startOn && !sess.DowntimeStartWasOn {
			sess.DowntimeRequested = true
		}
		sess.DowntimeStartWasOn = startOn
	}
	if stopOK {
		if stopOn && !sess.DowntimeStopWasOn {
			sess.DowntimeRequested = false
		}
		sess.DowntimeStopWasOn = stopOn
	}
}

// updateAlarms reads every CASE_MCS_STATE_ALARM_<code> bit present in this
// poll. A bit missing from the poll keeps its last value, so a partial
// cycle doesn't clear an active fault.
func updateAlarms(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, tr map[string]string) {
	for name, reg := range tr {
		if !strings.HasPrefix(name, mcsAlarmPrefix) {
			continue
		}
		code := strings.ToLower(strings.TrimPrefix(name, mcsAlarmPrefix))
		if code == "" {
			continue
		}
		on, ok := readBoolField(jsonPayloads, reg)
		if !ok {
			continue
		}
		sess.Mutex.Lock()
		if sess.AlarmOn == nil {
			sess.AlarmOn = make(map[string]bool)
		}
		sess.AlarmOn[code] = on
		sess.Mutex.Unlock()
	}
}

// ── State machine ─────────────────────────────────────────────────────────

// resolveTarget: fault > downtime button > run > idle.
func resolveTarget(sess *session.Session, runEdge bool) mcsTarget {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()

	var active []string
	for code, on := range sess.AlarmOn {
		if on {
			active = append(active, code)
		}
	}
	sort.Strings(active)

	switch {
	case len(active) > 0:
		// With momentary buttons nothing tells us the machine stopped, so
		// a fault cancels the run request: after the fault clears the
		// machine is idle until START is pressed again. With a level bit
		// the PLC's own run bit stays the truth.
		if runEdge {
			sess.RunRequested = false
		}
		return mcsTarget{state: mcsStateDowntime, reason: mcsReasonFault, alarms: active}
	case sess.DowntimeRequested:
		return mcsTarget{state: mcsStateDowntime, reason: mcsReasonButton}
	case sess.RunRequested:
		return mcsTarget{state: mcsStateRunning}
	default:
		return mcsTarget{state: mcsStateIdle}
	}
}

// applyTransition moves the session to target (if it changed), stores the
// transition fields in the session and returns their keys for processPatch.
func applyTransition(sess *session.Session, t mcsTarget, now time.Time) []string {
	sess.Mutex.Lock()
	prev := sess.MachineState
	since := sess.StateSince
	if prev == t.state {
		sess.Mutex.Unlock()
		return nil
	}
	sess.MachineState = t.state
	sess.StateSince = now
	sess.Mutex.Unlock()

	ts := now.Format(mcsTimeLayout)
	fields := map[string]any{"machine_state": t.state}

	// Timestamp key the trigger reads for each case.
	switch {
	case t.state == mcsStateRunning:
		fields["machine_start"] = ts
	case prev == mcsStateRunning:
		fields["machine_stop"] = ts // running -> idle / downtime
	default:
		fields["state_changed_at"] = ts // idle <-> downtime, downtime -> idle, cold start
	}

	// prev_* only when we actually saw the previous state start
	// (not on the first transition after a restart).
	if prev != "" && !since.IsZero() {
		d := math.Round(now.Sub(since).Seconds())
		fields["prev_state"] = prev
		fields["prev_duration_sec"] = d
		if prev == mcsStateRunning {
			fields["run_duration_sec"] = d
		}
	}

	if t.state == mcsStateDowntime {
		fields["reason"] = t.reason
		if len(t.alarms) > 0 {
			fields["alarms"] = t.alarms
		}
	}

	keys := make([]string, 0, len(fields))
	keys = append(keys, "machine_state")
	for k, v := range fields {
		storeJobFieldToSession(sess, k, v)
		if k != "machine_state" {
			keys = append(keys, k)
		}
	}

	log.Printf("handleMCSStateCase: %s -> %s %v", orDash(prev), t.state, fields)
	return keys
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}