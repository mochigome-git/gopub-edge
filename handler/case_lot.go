package handler

import (
	"log"
	"os"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
)

// handleLotCase fires exactly once on the OFF->ON edge of the configured
// trigger bit, publishing this device's machine_id so gokafka-raw can
// insert a new production.lot row.
func handleLotCase(
	session *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	buttonVal, exists := jsonPayloads.Get(tk.TriggerKey)
	if !exists {
		return
	}
	var buttonOn bool
	switch v := buttonVal.(type) {
	case float64:
		buttonOn = v == 1
	case string:
		buttonOn = v == "1"
	case bool:
		buttonOn = v
	default:
		log.Printf("handleLotCase: trigger key %q has unexpected type %T (value %v) — ignoring message", tk.TriggerKey, buttonVal, buttonVal)
		return
	}

	session.Mutex.Lock()
	wasOn := session.LotTriggerPrev
	session.LotTriggerPrev = buttonOn
	session.Mutex.Unlock()

	if !buttonOn || wasOn {
		// only act on the OFF->ON edge, ignore held/repeated ON scans
		return
	}

	machineID := os.Getenv("MACHINE_ID")
	if machineID == "" {
		log.Printf("handleLotCase: MACHINE_ID not set — skipping lot creation")
		return
	}

	session.Mutex.Lock()
	session.ProcessedPayloadsMap = map[string]map[string]any{
		"machine_id": {"machine_id": machineID},
	}
	keys := []string{"machine_id"}
	session.Mutex.Unlock()

	processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil, true)

	session.Mutex.Lock()
	session.ProcessedPayloadsMap = make(map[string]map[string]any)
	session.Mutex.Unlock()
}
