package handler

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopub-edge/internal/utils"
)

// Carry-forward: complete every batch with each register's last value.
//
// msp-edge publishes every register as its own MQTT message and a batch
// (cycle buffer or burst flusher) often closes mid-scan, so one batch holds
// e.g. 128 of 141 registers and the rest land in the next one. Every case
// used to see only that batch, so a register that happened to fall in the
// other half looked absent:
//
//   - lot (LOT_CONDITION_MODE=now): condition "not received" → lot ignored
//   - job: start/end date parts missing → time.Now(); setup fields empty
//   - job/lot/mcs: trigger register missing → that batch skipped
//   - timeseries: batch rejected by TIMESERIES_MIN_REGISTERS
//
// ProcessMQTTData now merges every batch into a last-value cache and fills
// the registers missing from the batch with their cached value, as long as
// it is younger than CARRY_MAX_AGE_SEC (default 10 s) — a register that
// really stopped arriving drops out instead of being repeated forever.
// Filled keys are marked (SafeJsonPayloads.IsCarried) so pulse logic
// (TIMESERIES_HOLD_ON) only counts a press from a fresh value.
// `messages` passed to the cases stays the raw batch.
//
// Level triggers of the legacy filling / vacuum / timing cases act on EVERY
// batch while the trigger reads 1, so they read the trigger with freshGet
// (received in this batch only) — a carried 1 must not run them again.
// Edge triggers (lot) and the debounced job trigger are fine with carried
// values: a repeated level is not a new edge.
//
// .env:
//   CARRY_FORWARD=false      turn it off (cases see the raw batch again)
//   CARRY_MAX_AGE_SEC=10     how long a value may be carried

type carriedValue struct {
	v  any
	at time.Time
}

type registerCache struct {
	mu   sync.Mutex
	vals map[string]carriedValue
}

var batchCache = &registerCache{}

// fill updates the cache with the batch's (fresh) keys, then fills every
// missing key younger than maxAge into p as carried. Returns how many were
// filled.
func (c *registerCache) fill(p *utils.SafeJsonPayloads, now time.Time, maxAge time.Duration) int {
	fresh := p.GetData()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.vals == nil {
		c.vals = make(map[string]carriedValue, len(fresh))
	}
	for k, v := range fresh {
		c.vals[k] = carriedValue{v: v, at: now}
	}

	filled := 0
	for k, cv := range c.vals {
		if now.Sub(cv.at) > maxAge {
			delete(c.vals, k) // stopped arriving
			continue
		}
		if _, ok := fresh[k]; ok {
			continue
		}
		p.SetCarried(k, cv.v)
		filled++
	}
	return filled
}

// carryForward fills p from the cache (no-op with CARRY_FORWARD=false).
func carryForward(p *utils.SafeJsonPayloads) {
	if !carryForwardEnabled() {
		return
	}
	n := batchCache.fill(p, time.Now(), carryMaxAge())
	if carryDebug && n > 0 {
		log.Printf("[carry] filled %d register(s) from earlier batches", n)
	}
}

var carryDebug = os.Getenv("CARRY_DEBUG") == "true"

// carryForwardEnabled: on unless CARRY_FORWARD=false.
func carryForwardEnabled() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("CARRY_FORWARD")), "false")
}

// carryMaxAge: CARRY_MAX_AGE_SEC, default 10 s.
func carryMaxAge() time.Duration {
	if sec, err := strconv.Atoi(os.Getenv("CARRY_MAX_AGE_SEC")); err == nil && sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return 10 * time.Second
}

// freshGet is Get, but a carried value counts as absent.
func freshGet(p *utils.SafeJsonPayloads, key string) (any, bool) {
	if p.IsCarried(key) {
		return nil, false
	}
	return p.Get(key)
}
