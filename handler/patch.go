package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"gopub-edge/config"
	"gopub-edge/internal/app"
	"gopub-edge/internal/session"
	"gopub-edge/patch"
	"log"
	"time"
)

func processPatch(session *session.Session, keys []string, cfg config.AppConfig, after func(), rMsgJSONChan <-chan string, plcApp *app.Application) {
	fmt.Println("All weight triggers are now inactive. Processing the patch.")

	parts := []map[string]any{}
	for _, key := range keys {
		parts = append(parts, session.ProcessedPayloadsMap[key])
	}
	data := mergeNonEmptyMaps(parts...)

	// Count top-level nil values
	nullCount := 0
	for _, value := range data {
		if value == nil {
			nullCount++
		}
	}
	if nullCount > 3 {
		fmt.Println("Aborting patch: more than 3 null values in data")
		resetWeightTriggers(session)
		if after != nil {
			after()
		}
		drainChannel(rMsgJSONChan)
		return
	}

	startTime := time.Now()
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	if cfg.InsertMode == "upsert" {
		_, err := patch.SendUpsertRequest(jsonData, cfg, plcApp, cfg.ReplyTimeout)
		if err != nil {
			log.Println("Error sending upsert request:", err)
		}
	} else {
		if err := patch.SendPatchRequest(jsonData); err != nil {
			log.Println("Error publishing patch request:", err)
			return
		}

	}

	prettyPrintJSONWithTime(data, time.Since(startTime))

	session.Mutex.Lock()
	for key := range session.ProcessedPayloadsMap {
		delete(session.ProcessedPayloadsMap, key)
	}
	// Reset remark latch streak counters alongside the payload map so the
	// next sequence starts fresh.
	for key := range session.RemarkNormalStreak {
		delete(session.RemarkNormalStreak, key)
	}
	session.Mutex.Unlock()

	// Always reset weight triggers
	resetWeightTriggers(session)

	// Call the extra cleanup if provided
	if after != nil {
		after()
	}

	drainChannel(rMsgJSONChan)

	if plcApp != nil {
		err := plcApp.WritePLC(context.Background(), cfg.Plc.PlcDevice, cfg.Plc.PlcData)
		if err != nil {
			fmt.Println("PLC write failed:", err)
		}
	}

}

func shouldPatch(caseID string, ready bool, session *session.Session) bool {
	if caseID == "case7" || caseID == "case8" || caseID == "case9" || caseID == "case10" {
		// Case 7 & Case 8: Wait for all channels to deactivate after being active
		return !session.WeightTriggerCh1 && !session.WeightTriggerCh2 && !session.WeightTriggerCh3 &&
			session.PrevWeightTriggerCh1 && session.PrevWeightTriggerCh2 && session.PrevWeightTriggerCh3 && ready
	}
	// Default: don't patch
	return false
}

// Reset previous triggers to avoid reprocessing
func resetWeightTriggers(session *session.Session) {
	session.AllSuccessZero = false
	session.IsProcessing = false
	session.PrevWeightTriggerCh1 = false
	session.PrevWeightTriggerCh2 = false
	session.PrevWeightTriggerCh3 = false
	*session.PrevWeightValueCh1 = 0
	*session.PrevWeightValueCh2 = 0
	*session.PrevWeightValueCh3 = 0
}
