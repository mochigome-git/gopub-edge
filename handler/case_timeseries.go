package handler

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/utils"
	"gopub-edge/patch"
)

// CASE timeseries: publish the latest COMPLETE PLC scan cycle on a fixed
// interval (default 3s).
//
//   - Completeness comes from the CycleBuffer (anchor topic repeat = 1 cycle).
//     handleTimeseriesCase only stores a snapshot if the batch has at least
//     TIMESERIES_MIN_REGISTERS registers, so a partial batch (e.g. from the
//     legacy burst flusher still feeding the same channel) is ignored.
//   - Cadence comes from a single ticker goroutine, NOT from cycle arrival,
//     so the publish interval stays at TIMESERIES_INTERVAL_SEC regardless of
//     how fast/slow/jittery the PLC scan is.
//   - A cycle is published at most once (seq check): if no new complete
//     cycle arrived since the last tick, the tick is skipped rather than
//     re-sending stale data.
//
// No top-level "kind" is set, so gokafka-raw treats it as plain telemetry:
// telemetry_raw insert + InsertRealtimeMetric for any active metric config
// row with method='realtime' for this tenant/device.
//
// .env:
//   TRIGGER=ts,timeseries                 (first element is just a label)
//   TIMESERIES_INTERVAL_SEC=3
//   TIMESERIES_MIN_REGISTERS=100          (optional, partial-batch guard)
//   TIMESERIES_EXCLUDE=d0,d1              (optional)
//   CASE_TS_<output_key>=<plc_register>   (optional rename, e.g. CASE_TS_temperature=d200)

const timeseriesPrefix = "CASE_TS_"

type timeseriesSampler struct {
	mu       sync.Mutex
	latest   map[string]any
	latestAt time.Time
	seq      uint64 // bumped on every accepted complete cycle
	sentSeq  uint64 // seq of the last published cycle
	lastSent time.Time
}

var (
	tsSampler   = &timeseriesSampler{}
	tsStartOnce sync.Once
	tsStop      = make(chan struct{})
	tsStopOnce  sync.Once

	tsLastRejectLog time.Time // only touched from the handler goroutine
	tsPublished     uint64    // only touched from the sampler goroutine
)

// handleTimeseriesCase is called once per batch from Trigger(). It never
// publishes directly — it only replaces the latest complete snapshot.
func handleTimeseriesCase(jsonPayloads *utils.SafeJsonPayloads, cfg config.AppConfig) {
	tsStartOnce.Do(func() {
		interval := timeseriesInterval()
		go tsSampler.run(cfg, interval)
		log.Printf("[timeseries] ✓ sampler started — every %v", interval)
	})

	snapshot, ok := buildTimeseriesSnapshot(jsonPayloads)
	if !ok {
		// partial batch, keep the previous complete snapshot — but say so
		// (rate-limited) so a too-high TIMESERIES_MIN_REGISTERS is visible.
		if time.Since(tsLastRejectLog) > 30*time.Second {
			tsLastRejectLog = time.Now()
			log.Printf("[timeseries] ⚠ batch rejected — %d registers, TIMESERIES_MIN_REGISTERS=%d",
				len(jsonPayloads.GetData()), timeseriesMinRegisters())
		}
		return
	}

	tsSampler.mu.Lock()
	tsSampler.latest = snapshot
	tsSampler.latestAt = time.Now()
	tsSampler.seq++
	tsSampler.mu.Unlock()
}

// buildTimeseriesSnapshot takes EVERY register in the cycle as-is (raw
// lowercase address -> value). No per-register mapping is required.
//
//   - CASE_TS_<name>=<register> is OPTIONAL: only for the few registers you
//     want renamed (e.g. CASE_TS_temperature=d200 sends "temperature"
//     instead of "d200"). Unlisted registers keep their raw address.
//   - TIMESERIES_EXCLUDE=d0,d1,m100 drops registers you don't want sent.
//   - TIMESERIES_MIN_REGISTERS=100 rejects any batch with fewer registers
//     (a partial batch, e.g. from the legacy burst flusher). 0/unset = off.
//
// Zero and non-numeric values are kept — a 0 reading is a real sample.
func buildTimeseriesSnapshot(jsonPayloads *utils.SafeJsonPayloads) (map[string]any, bool) {
	raw := jsonPayloads.GetData()

	if min := timeseriesMinRegisters(); min > 0 && len(raw) < min {
		return nil, false
	}

	exclude := map[string]bool{}
	for _, r := range strings.Split(os.Getenv("TIMESERIES_EXCLUDE"), ",") {
		if r = strings.ToLower(strings.TrimSpace(r)); r != "" {
			exclude[r] = true
		}
	}

	// register -> output name, from the optional CASE_TS_ renames
	rename := map[string]string{}
	for newKey, register := range utils.GetKeyTransformationsFromEnv(timeseriesPrefix) {
		rename[strings.ToLower(register)] = strings.ToLower(newKey)
	}

	out := make(map[string]any, len(raw))
	for register, v := range raw {
		register = strings.ToLower(register)
		if exclude[register] {
			continue
		}
		if name, ok := rename[register]; ok {
			out[name] = v
		} else {
			out[register] = v
		}
	}

	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func timeseriesMinRegisters() int {
	n, err := strconv.Atoi(os.Getenv("TIMESERIES_MIN_REGISTERS"))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *timeseriesSampler) run(cfg config.AppConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-tsStop:
			log.Println("[timeseries] sampler stopped")
			return

		case now := <-ticker.C:
			s.mu.Lock()
			if s.latest == nil || s.seq == s.sentSeq {
				s.mu.Unlock()
				continue // no new complete cycle since last publish
			}
			data := utils.DeepCopyMap(s.latest)
			sampledAt := s.latestAt
			s.sentSeq = s.seq
			prev := s.lastSent
			s.lastSent = now
			s.mu.Unlock()

			if !prev.IsZero() {
				if gap := now.Sub(prev); gap > interval+interval/2 {
					log.Printf("[timeseries] ⚠ gap %v since last publish (expected %v) — PLC cycle slower than interval or cycles dropped",
						gap.Round(time.Millisecond), interval)
				}
			}

			// shapeReadingsEnvelope (not buildReadingsEnvelope): no
			// good/reject/total tally — counting remarks every 3s would
			// inflate counts if anything downstream sums them.
			envelope := shapeReadingsEnvelope(data, cfg)

			status, _ := envelope["status"].(map[string]any)
			if status == nil {
				status = map[string]any{}
			}
			status["ts"] = sampledAt.UTC().Format(time.RFC3339Nano)
			envelope["status"] = status

			if err := patch.SendPatchRequest(envelope); err != nil {
				log.Printf("[timeseries] ⚠ publish failed: %v", err)
				continue
			}

			// First publish, then once a minute (every 20th at 3s), or every
			// publish with TIMESERIES_DEBUG=true.
			tsPublished++
			if tsPublished == 1 || tsPublished%20 == 0 || os.Getenv("TIMESERIES_DEBUG") == "true" {
				log.Printf("[timeseries] ✓ published #%d — %d registers, sampled %s",
					tsPublished, len(data), sampledAt.Format("15:04:05.000"))
			}
		}
	}
}

// StopTimeseries stops the sampler goroutine. Safe to call more than once
// and safe to call if the timeseries case was never used.
func StopTimeseries() {
	tsStopOnce.Do(func() { close(tsStop) })
}

func timeseriesInterval() time.Duration {
	sec, err := strconv.Atoi(os.Getenv("TIMESERIES_INTERVAL_SEC"))
	if err != nil || sec <= 0 {
		sec = 3
	}
	return time.Duration(sec) * time.Second
}
