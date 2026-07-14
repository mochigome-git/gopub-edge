// Heartbeat support for Publisher. Reuses the existing fire-and-forget
// Publish path (no reply_topic attached) so heartbeats flow through the
// exact same request topic and downstream insert engine as everything
// else — no special-casing needed on the receiving side. The row lands
// with tenant_id/device_id/status populated and readings/output/limits/
// energy/lot_id left null, same shape the device-side ESP32 heartbeats
// already produce (status.kind = "heartbeat").
package mqttpub

import (
	"log"
	"time"
)

// StartHeartbeat launches a goroutine that publishes a heartbeat message
// every interval until stop is closed. tenantID/deviceID identify this
// gopub-edge instance the same way its normal readings publishes do.
// version is a free-form string (e.g. config.AppVersion) reported as
// status.fw. startTime should be captured at process boot so uptime_s
// reflects how long this gopub-edge process has been running.
//
// Call this once after NewPublisher succeeds, passing a fresh
// chan struct{} the caller closes on shutdown (same pattern as
// stopFlusher/stopCycle in mqtts.go) — StartHeartbeat itself returns
// immediately, the ticker runs in the background.
func (p *Publisher) StartHeartbeat(stop <-chan struct{}, tenantID, deviceID, version string, startTime time.Time, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Printf("[mqttpub] ✓ heartbeat started — every %v (tenant_id=%s device_id=%s)", interval, tenantID, deviceID)

		for {
			select {
			case <-stop:
				log.Println("[mqttpub] heartbeat stopped")
				return
			case <-ticker.C:
				hb := map[string]any{
					"tenant_id": tenantID,
					"device_id": deviceID,
					"status": map[string]any{
						"kind":     "heartbeat",
						"fw":       version,
						"ts":       time.Now().UTC().Format(time.RFC3339),
						"uptime_s": int64(time.Since(startTime).Seconds()),
					},
				}
				if err := p.Publish(hb); err != nil {
					log.Printf("[mqttpub] ⚠ heartbeat publish failed: %v", err)
				} else {
					log.Printf("[mqttpub] ♥ heartbeat published (topic=%s uptime_s=%v)", p.requestTopic, hb["status"].(map[string]any)["uptime_s"])
				}
			}
		}
	}()
}
