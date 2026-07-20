package handler

import (
	"context"
	"log"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/app"
	"gopub-edge/internal/session"
	"gopub-edge/patch"
)

func processPatch(session *session.Session, keys []string, cfg config.AppConfig, after func(), rMsgJSONChan <-chan string, plcApp *app.Application) {
	session.Mutex.Lock()
	session.IsProcessing = true
	session.Mutex.Unlock()

	log.Println("[patch] All weight triggers are now inactive. Processing the patch.")

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
		log.Println("[patch] Aborting patch: more than 3 null values in data")
		resetWeightTriggers(session)
		if after != nil {
			after()
		}
		drainChannel(rMsgJSONChan)
		return
	}

	envelope := buildReadingsEnvelope(data, cfg)

	startTime := time.Now()

	// Decide upsert-vs-patch per call, based on whether the caller passed
	// a plcApp — not off cfg.InsertMode globally. Cases that pass nil
	// (6/7/8/9, WeightMCS) want fire-and-forget; a case that passes a real
	// plcApp (e.g. the Vacuum healthcheck case) wants the reply so it can
	// write X/Y/vacuum status back to the PLC. A single global InsertMode
	// can't represent both at once.
	//
	// NOTE: publish/reply failures here are treated as transient (network
	// blip, reply engine restart, timeout) and only logged. The old HTTP
	// path used log.Fatal, which made sense for a synchronous call that
	// either succeeded or definitively failed — but panicking gopatch on
	// every MQTT hiccup would take the whole ingestion pipeline down.
	if plcApp != nil {
		_, err := patch.SendUpsertRequest(envelope, cfg, plcApp, cfg.ReplyTimeout)
		if err != nil {
			log.Println("[upsert] Error sending upsert request:", err)
		}
	} else {
		if err := patch.SendPatchRequest(envelope); err != nil {
			log.Println("[patch] Error publishing patch request:", err)
		}
	}

	prettyPrintJSONWithTime(envelope, time.Since(startTime))

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
			log.Println("PLC write failed:", err)
		}
	}
}

func shouldPatch(caseID string, ready bool, session *session.Session) bool {
	session.Mutex.Lock()
	alreadyProcessing := session.IsProcessing
	session.Mutex.Unlock()
	if alreadyProcessing {
		return false
	}

	if caseID == "case7" || caseID == "case8" || caseID == "case9" || caseID == "case10" {
		return !session.WeightTriggerCh1 && !session.WeightTriggerCh2 && !session.WeightTriggerCh3 &&
			session.PrevWeightTriggerCh1 && session.PrevWeightTriggerCh2 && session.PrevWeightTriggerCh3 && ready
	}
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
