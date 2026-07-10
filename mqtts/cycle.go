package mqtts

import (
	"fmt"
	"gopub-edge/model"
	"sync"
	"time"
)

// CycleBuffer detects PLC scan cycle boundaries by watching for the first topic to repeat.
// When the anchor topic is seen again, it signals that one full cycle of D-device data is complete.
type CycleBuffer struct {
	mu          sync.Mutex
	firstTopic  string               // anchor topic — must be the very first topic the PLC publishes each scan
	buffer      []model.Message      // accumulates messages within the current cycle
	cycleReady  chan []model.Message // signals a complete cycle to consumers
	seen        bool                 // true after we've seen the anchor topic at least once
	timeout     time.Duration        // safety flush if anchor topic stops arriving
	lastAnchor  time.Time            // time of last anchor topic seen
	stopTimeout chan struct{}        // signals timeout goroutine to stop
}

// NewCycleBuffer creates a CycleBuffer with the given anchor topic.
// firstTopic must be the very first D-device topic your PLC publishes each scan cycle.
// timeout is a safety net — if the anchor topic is not seen within this duration,
// the buffer will be force-flushed. Pass 0 to disable the timeout.
func NewCycleBuffer(firstTopic string, timeout time.Duration) *CycleBuffer {
	cb := &CycleBuffer{
		firstTopic:  firstTopic,
		cycleReady:  make(chan []model.Message, 4), // buffer up to 4 unprocessed cycles
		timeout:     timeout,
		stopTimeout: make(chan struct{}),
	}

	if timeout > 0 {
		go cb.runTimeoutFlusher()
	}

	return cb
}

// Feed receives each incoming MQTT message and its topic.
// Call this from your MQTT message handler for every message.
func (cb *CycleBuffer) Feed(topic string, msg model.Message) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if topic == cb.firstTopic {
		cb.lastAnchor = time.Now()

		// If we have already seen the anchor topic before and have buffered data,
		// the previous cycle is complete — ship it out.
		if cb.seen && len(cb.buffer) > 0 {
			cb.flushLocked(fmt.Sprintf("cycle complete (%d messages)", len(cb.buffer)))
		}

		cb.seen = true
	}

	// Only accumulate messages after we have seen the anchor topic at least once.
	// This ensures we never process a partial first cycle.
	if cb.seen {
		cb.buffer = append(cb.buffer, msg)
	}
}

// Cycles returns a read-only channel that emits a complete []model.Message slice
// for every finished PLC scan cycle. Consume this in a goroutine.
func (cb *CycleBuffer) Cycles() <-chan []model.Message {
	return cb.cycleReady
}

// Stop shuts down the background timeout flusher goroutine.
// Call this during graceful shutdown.
func (cb *CycleBuffer) Stop() {
	close(cb.stopTimeout)
}

// Stats returns diagnostic information about the buffer state.
func (cb *CycleBuffer) Stats() map[string]interface{} {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	timeSinceAnchor := time.Duration(0)
	if !cb.lastAnchor.IsZero() {
		timeSinceAnchor = time.Since(cb.lastAnchor)
	}

	return map[string]interface{}{
		"buffered_messages":    len(cb.buffer),
		"cycle_channel_depth":  len(cb.cycleReady),
		"seen_anchor":          cb.seen,
		"time_since_anchor_ms": timeSinceAnchor.Milliseconds(),
	}
}

// flushLocked snapshots the current buffer and sends it on cycleReady.
// Must be called with cb.mu held.
func (cb *CycleBuffer) flushLocked(reason string) {
	cycle := make([]model.Message, len(cb.buffer))
	copy(cycle, cb.buffer)
	cb.buffer = cb.buffer[:0]

	select {
	case cb.cycleReady <- cycle:
		fmt.Printf("[CycleBuffer] ✓ %s\n", reason)
	default:
		// Consumer is too slow — drop the cycle to avoid blocking the MQTT handler.
		// Increase channel buffer in NewCycleBuffer if this happens frequently.
		fmt.Printf("[CycleBuffer] ⚠ Cycle dropped (consumer too slow): %s\n", reason)
	}
}

// runTimeoutFlusher periodically checks if the anchor topic has gone silent.
// If no anchor has been seen within cb.timeout, it force-flushes the buffer.
// This guards against partial cycles when the PLC restarts or skips a topic.
func (cb *CycleBuffer) runTimeoutFlusher() {
	// Check twice per timeout duration for responsiveness
	ticker := time.NewTicker(cb.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-cb.stopTimeout:
			// Final flush on shutdown
			cb.mu.Lock()
			if len(cb.buffer) > 0 {
				cb.flushLocked(fmt.Sprintf("shutdown flush (%d messages)", len(cb.buffer)))
			}
			cb.mu.Unlock()
			return

		case <-ticker.C:
			cb.mu.Lock()
			if cb.seen && len(cb.buffer) > 0 && !cb.lastAnchor.IsZero() {
				if time.Since(cb.lastAnchor) >= cb.timeout {
					cb.flushLocked(fmt.Sprintf("timeout flush (%d messages, anchor silent for %v)",
						len(cb.buffer), time.Since(cb.lastAnchor).Round(time.Millisecond)))
					// Reset seen so we wait for a clean anchor before accumulating again
					cb.seen = false
				}
			}
			cb.mu.Unlock()
		}
	}
}
