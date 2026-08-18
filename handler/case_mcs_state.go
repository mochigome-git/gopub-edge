// case_mcs_state.go
package handler

import (
	"log"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

// mcsStateFieldPrefix drives the machine run/downtime state machine.
// Each role is its own env var, so the two pairs (run start/stop,
// downtime start/stop) can independently be wired to either two
// separate momentary buttons or a single shared level bit:
//
//	// two separate buttons — each is read as its own rising-edge pulse
//	CASE_MCS_STATE_RUN_START=m300
//	CASE_MCS_STATE_RUN_STOP=m301
//
//	// one physical bit doing both jobs — point both roles at the SAME
//	// register; the handler detects this and falls back to ON/OFF
//	// level semantics (bit=1 -> running, bit=0 -> idle), same pattern
//	// job_case.go uses for its single job button
//	CASE_MCS_STATE_RUN_START=m300
//	CASE_MCS_STATE_RUN_STOP=m300
//
// The exact same rule applies to CASE_MCS_STATE_DOWNTIME_START /
// CASE_MCS_STATE_DOWNTIME_STOP. DOWNTIME_* is optional — if you don't
// configure it, the case only ever reports running/idle.
const mcsStateFieldPrefix = "CASE_MCS_STATE_"

const (
	mcsStateRunning  = "running"
	mcsStateIdle     = "idle"
	mcsStateDowntime = "downtime"
)

// ─────────────────────────────────────────────────────────────────────────
// REQUIRED session.Session additions (add these to internal/session):
//
//	MachineState       string    // "running" | "idle" | "downtime"
//	PreDowntimeState   string    // state to restore when downtime ends
//	RunStartWasOn      bool
//	RunStopWasOn       bool
//	DowntimeStartWasOn bool
//	DowntimeStopWasOn  bool
//
// These follow the same edge-tracking pattern as JobInProgress in
// session.Session already used by job_case.go.
// ─────────────────────────────────────────────────────────────────────────

func handleMCSStateCase(
	sess *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	transformations := utils.GetKeyTransformationsFromEnv(mcsStateFieldPrefix)

	runStartKey, hasRunStart := transformations["RUN_START"]
	runStopKey, hasRunStop := transformations["RUN_STOP"]
	downStartKey, hasDownStart := transformations["DOWNTIME_START"]
	downStopKey, hasDownStop := transformations["DOWNTIME_STOP"]

	if !hasRunStart {
		log.Printf("handleMCSStateCase: no RUN_START register configured (CASE_MCS_STATE_RUN_START) — skipping")
		return
	}

	var keys []string

	// ── Run state (running / idle) ──────────────────────────────────
	if hasRunStop && runStopKey != runStartKey {
		keys = append(keys, handleRunEdgeButtons(sess, jsonPayloads, runStartKey, runStopKey)...)
	} else {
		keys = append(keys, handleRunLevelButton(sess, jsonPayloads, runStartKey)...)
	}

	// ── Downtime overlay (optional) ─────────────────────────────────
	if hasDownStart {
		if hasDownStop && downStopKey != downStartKey {
			keys = append(keys, handleDowntimeEdgeButtons(sess, jsonPayloads, downStartKey, downStopKey)...)
		} else {
			keys = append(keys, handleDowntimeLevelButton(sess, jsonPayloads, downStartKey)...)
		}
	}

	if len(keys) == 0 {
		return
	}

	// machine_state changed at least once this poll — always include it.
	keys = append([]string{"machine_state"}, keys...)

	// false: these fields belong in the readings table (buildReadingsEnvelope
	// -> status bucket, see statusKeys in envelope.go), not analytics.job_summary
	// (buildJobEnvelope), which is what the trailing `true` selects for job_case.
	processPatch(sess, keys, cfg, nil, rMsgJSONChan, nil)
}

// readBoolField mirrors the button-value coercion job_case.go uses for
// its trigger button, applied here per register key.
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

func setMachineState(sess *session.Session, state string) {
	sess.Mutex.Lock()
	sess.MachineState = state
	sess.Mutex.Unlock()
	storeJobFieldToSession(sess, "machine_state", state)
}

func stampField(sess *session.Session, field string) {
	storeJobFieldToSession(sess, field, time.Now().UTC().Format(time.RFC3339))
}

// handleRunLevelButton: single shared bit for RUN_START/RUN_STOP — a
// level signal that stays 1 while running and drops to 0 otherwise.
// Rising edge -> running, falling edge -> idle. If downtime is active,
// the run/idle transition is remembered (PreDowntimeState) but not
// surfaced, so downtime ending hands control back to the right state.
func handleRunLevelButton(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, key string) []string {
	on, exists := readBoolField(jsonPayloads, key)
	if !exists {
		return nil
	}

	sess.Mutex.Lock()
	wasOn := sess.RunStartWasOn
	sess.RunStartWasOn = on
	inDowntime := sess.MachineState == mcsStateDowntime
	sess.Mutex.Unlock()

	switch {
	case on && !wasOn:
		if inDowntime {
			sess.Mutex.Lock()
			sess.PreDowntimeState = mcsStateRunning
			sess.Mutex.Unlock()
			return nil
		}
		setMachineState(sess, mcsStateRunning)
		stampField(sess, "machine_start")
		return []string{"machine_start"}
	case !on && wasOn:
		if inDowntime {
			sess.Mutex.Lock()
			sess.PreDowntimeState = mcsStateIdle
			sess.Mutex.Unlock()
			return nil
		}
		setMachineState(sess, mcsStateIdle)
		stampField(sess, "machine_stop")
		return []string{"machine_stop"}
	}
	return nil
}

// handleRunEdgeButtons: separate RUN_START / RUN_STOP momentary
// buttons. Each firing is an independent event.
func handleRunEdgeButtons(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, startKey, stopKey string) []string {
	startOn, startExists := readBoolField(jsonPayloads, startKey)
	stopOn, stopExists := readBoolField(jsonPayloads, stopKey)

	sess.Mutex.Lock()
	startWasOn := sess.RunStartWasOn
	stopWasOn := sess.RunStopWasOn
	if startExists {
		sess.RunStartWasOn = startOn
	}
	if stopExists {
		sess.RunStopWasOn = stopOn
	}
	inDowntime := sess.MachineState == mcsStateDowntime
	sess.Mutex.Unlock()

	var keys []string

	if startExists && startOn && !startWasOn {
		if inDowntime {
			sess.Mutex.Lock()
			sess.PreDowntimeState = mcsStateRunning
			sess.Mutex.Unlock()
		} else {
			setMachineState(sess, mcsStateRunning)
			stampField(sess, "machine_start")
			keys = append(keys, "machine_start")
		}
	}

	if stopExists && stopOn && !stopWasOn {
		if inDowntime {
			sess.Mutex.Lock()
			sess.PreDowntimeState = mcsStateIdle
			sess.Mutex.Unlock()
		} else {
			setMachineState(sess, mcsStateIdle)
			stampField(sess, "machine_stop")
			keys = append(keys, "machine_stop")
		}
	}

	return keys
}

// handleDowntimeLevelButton: single shared bit for downtime start/stop.
func handleDowntimeLevelButton(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, key string) []string {
	on, exists := readBoolField(jsonPayloads, key)
	if !exists {
		return nil
	}

	sess.Mutex.Lock()
	wasOn := sess.DowntimeStartWasOn
	sess.DowntimeStartWasOn = on
	current := sess.MachineState
	sess.Mutex.Unlock()

	if on && !wasOn && current != mcsStateDowntime {
		sess.Mutex.Lock()
		sess.PreDowntimeState = current
		sess.Mutex.Unlock()
		setMachineState(sess, mcsStateDowntime)
		stampField(sess, "downtime_start")
		return []string{"downtime_start"}
	}

	if !on && wasOn && current == mcsStateDowntime {
		sess.Mutex.Lock()
		restore := sess.PreDowntimeState
		sess.Mutex.Unlock()
		if restore == "" {
			restore = mcsStateIdle
		}
		setMachineState(sess, restore)
		stampField(sess, "downtime_stop")
		return []string{"downtime_stop"}
	}

	return nil
}

// handleDowntimeEdgeButtons: separate DOWNTIME_START / DOWNTIME_STOP
// momentary buttons.
func handleDowntimeEdgeButtons(sess *session.Session, jsonPayloads *utils.SafeJsonPayloads, startKey, stopKey string) []string {
	startOn, startExists := readBoolField(jsonPayloads, startKey)
	stopOn, stopExists := readBoolField(jsonPayloads, stopKey)

	sess.Mutex.Lock()
	startWasOn := sess.DowntimeStartWasOn
	stopWasOn := sess.DowntimeStopWasOn
	if startExists {
		sess.DowntimeStartWasOn = startOn
	}
	if stopExists {
		sess.DowntimeStopWasOn = stopOn
	}
	current := sess.MachineState
	sess.Mutex.Unlock()

	var keys []string

	if startExists && startOn && !startWasOn && current != mcsStateDowntime {
		sess.Mutex.Lock()
		sess.PreDowntimeState = current
		sess.Mutex.Unlock()
		setMachineState(sess, mcsStateDowntime)
		stampField(sess, "downtime_start")
		keys = append(keys, "downtime_start")
	}

	if stopExists && stopOn && !stopWasOn && current == mcsStateDowntime {
		sess.Mutex.Lock()
		restore := sess.PreDowntimeState
		sess.Mutex.Unlock()
		if restore == "" {
			restore = mcsStateIdle
		}
		setMachineState(sess, restore)
		stampField(sess, "downtime_stop")
		keys = append(keys, "downtime_stop")
	}

	return keys
}
