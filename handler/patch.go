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

type envelopeKind string

const (
	envelopeReadings envelopeKind = "readings"
	envelopeJob      envelopeKind = "job"
	envelopeLot      envelopeKind = "lot"
)

func processPatch(session *session.Session, keys []string, cfg config.AppConfig, after func(), rMsgJSONChan <-chan string, plcApp *app.Application, kind ...envelopeKind) {
	processPatchFrom(session, session.ProcessedPayloadsMap, keys, cfg, after, rMsgJSONChan, plcApp, kind...)
}

// processPatchFrom is processPatch reading from src instead of
// session.ProcessedPayloadsMap. The job case passes a frozen snapshot so
// nothing a later frame does to the session can change what gets posted.
func processPatchFrom(session *session.Session, src map[string]map[string]any, keys []string, cfg config.AppConfig, after func(), rMsgJSONChan <-chan string, plcApp *app.Application, kind ...envelopeKind) {
	// Job and lot posts must NOT drain the input queue: rMsgJSONChan is the
	// main PLC-data channel, and draining it throws away every queued
	// batch — e.g. the next lot press, the first frames of the next job,
	// a state change. The drain only made sense for the old hold/weight
	// cases, which wanted to skip frames that piled up during a slow
	// synchronous HTTP call.
	drain := true
	if len(kind) > 0 && (kind[0] == envelopeJob || kind[0] == envelopeLot) {
		drain = false
	}
	processPatchOpts(session, src, keys, cfg, after, rMsgJSONChan, plcApp, drain, kind...)
}

// processPatchKeepQueue is processPatch without draining the input queue —
// for event cases (machine state) that must see every following frame.
func processPatchKeepQueue(session *session.Session, keys []string, cfg config.AppConfig, rMsgJSONChan <-chan string) {
	processPatchOpts(session, session.ProcessedPayloadsMap, keys, cfg, nil, rMsgJSONChan, nil, false)
}

func processPatchOpts(session *session.Session, src map[string]map[string]any, keys []string, cfg config.AppConfig, after func(), rMsgJSONChan <-chan string, plcApp *app.Application, drain bool, kind ...envelopeKind) {
	session.Mutex.Lock()
	session.IsProcessing = true
	session.Mutex.Unlock()

	log.Printf("[patch] publishing %v", keys)

	parts := []map[string]any{}
	for _, key := range keys {
		parts = append(parts, src[key])
	}
	data := mergeNonEmptyMaps(parts...)

	// Strip any raw, never-renamed PLC register (d243, w6e, m1540, y54a...)
	// that reached data via a bucket like "readings" that StoreFlattenedPayloadToSession
	// sweeps jsonPayloads leftovers into. These are leftovers from
	// Hold_changeName_generic not deleting its source key after copying
	// it elsewhere — not legitimate output. Filtering here, post-merge,
	// catches them regardless of which bucket they were nested under.
	for k := range data {
		if isRawRegisterKey(k) {
			delete(data, k)
		}
	}

	// Count top-level nil values
	nullCount := 0
	for _, value := range data {
		if value == nil {
			nullCount++
		}
	}
	isEvent := len(kind) > 0 && (kind[0] == envelopeJob || kind[0] == envelopeLot)
	if nullCount > 3 && isEvent {
		// A finished job or a new lot is a one-off record — never drop it
		// for having empty fields; drop the empty fields instead.
		log.Printf("[patch] %s has %d null fields — dropping them and posting anyway", kind[0], nullCount)
		for k, v := range data {
			if v == nil {
				delete(data, k)
			}
		}
		nullCount = 0
	}
	if nullCount > 3 {
		log.Println("[patch] Aborting patch: more than 3 null values in data")
		resetWeightTriggers(session)
		if after != nil {
			after()
		}
		if drain {
			drainChannel(rMsgJSONChan)
		}
		return
	}

	envKind := envelopeReadings
	if len(kind) > 0 {
		envKind = kind[0]
	}

	var envelope map[string]any
	switch envKind {
	case envelopeJob:
		envelope = buildJobEnvelope(data, cfg)
	case envelopeLot:
		envelope = buildLotEnvelope(data, cfg)
	default:
		envelope = buildReadingsEnvelope(data, cfg)
	}

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
	for _, key := range keys {
		delete(src, key)
	}
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
	if drain {
		drainChannel(rMsgJSONChan)
	}

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
