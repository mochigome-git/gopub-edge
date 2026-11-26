package handler

import (
	"log"
	"os"
	"strings"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

// handleLotCase fires exactly once on the OFF->ON edge of the configured
// trigger bit, publishing this device's machine_id so gokafka-raw can
// insert a new production.lot row.
//
// Optional CASE_LOT_REQUIRE_<register>=<condition> entries (any number,
// see lot_conditions.go) gate that edge: e.g. a meter-reset button only
// starts a new lot when the meter went over 1500 and the speed over 200
// during the lot that is ending. Without them every edge creates a lot.
func handleLotCase(
	session *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	// Record this poll's condition values first — including the trigger
	// poll itself, whose meter value may be the last before it is reset.
	conds := loadLotConditions()
	if len(conds) > 0 {
		updateLotConditions(session, conds, jsonPayloads, messages)
	}

	// A batch can hold many readings of the trigger (msp-edge polls several
	// times a second, the flusher batches up to 2 s), but jsonPayloads keeps
	// only the last one — a short press in the middle of a batch would be
	// lost. Walk every reading in order and look for an OFF->ON edge.
	values := triggerReadings(tk.TriggerKey, messages, jsonPayloads)
	if len(values) == 0 {
		return
	}

	session.Mutex.Lock()
	prev := session.LotTriggerPrev
	rising := false
	for _, on := range values {
		if on && !prev {
			rising = true
		}
		prev = on
	}
	session.LotTriggerPrev = prev
	session.Mutex.Unlock()

	if !rising {
		// only act on the OFF->ON edge, ignore held/repeated ON scans
		return
	}

	if len(conds) > 0 {
		mode := lotConditionMode()
		ok, unmet := lotConditionsSatisfied(session, conds, mode, jsonPayloads)
		if !ok {
			log.Printf("handleLotCase: %s ON ignored — lot continues; unmet (%s): %s",
				tk.TriggerKey, mode, strings.Join(unmet, ", "))
			return
		}
	}

	machineID := os.Getenv("MACHINE_ID")
	if machineID == "" {
		log.Printf("handleLotCase: MACHINE_ID not set — skipping lot creation")
		return
	}
	if len(conds) > 0 {
		log.Printf("handleLotCase: %s ON — all %d conditions met, creating new lot", tk.TriggerKey, len(conds))
		resetLotConditions(session)
	}

	publishLot(session, machineID, cfg, rMsgJSONChan)
}

// publishLot sends the lot envelope. A variable so tests can count lots
// without an MQTT publisher.
var publishLot = func(session *session.Session, machineID string, cfg config.AppConfig, rMsgJSONChan <-chan string) {
	session.Mutex.Lock()
	session.ProcessedPayloadsMap["machine_id"] = map[string]any{"machine_id": machineID}
	keys := []string{"machine_id"}
	session.Mutex.Unlock()

	processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil, envelopeLot)

	session.Mutex.Lock()
	session.ProcessedPayloadsMap = make(map[string]map[string]any)
	session.Mutex.Unlock()
}

// triggerReadings returns every reading of the trigger register in this
// batch, oldest first. Falls back to the merged payload value when the raw
// messages aren't available.
func triggerReadings(key string, messages []model.Message, jsonPayloads *utils.SafeJsonPayloads) []bool {
	var out []bool
	for _, m := range messages {
		if !strings.EqualFold(m.Address, key) {
			continue
		}
		if on, ok := buttonOn(key, m.Value); ok {
			out = append(out, on)
		}
	}
	if len(out) > 0 {
		return out
	}
	if v, exists := jsonPayloads.Get(key); exists {
		if on, ok := buttonOn(key, v); ok {
			out = append(out, on)
		}
	}
	return out
}

func buttonOn(key string, v any) (bool, bool) {
	switch x := v.(type) {
	case float64:
		return x == 1, true
	case string:
		return x == "1", true
	case bool:
		return x, true
	default:
		log.Printf("handleLotCase: trigger key %q has unexpected type %T (value %v) — ignoring", key, v, v)
		return false, false
	}
}
