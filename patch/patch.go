package patch

import (
	"fmt"

	"gopub-edge/internal/mqttpub"
)

// Pub is the shared MQTT publisher used by SendPatchRequest and
// SendUpsertRequest. Set this once at startup, e.g. in main.go:
//
//	pub, err := mqttpub.NewPublisher(opts, cfg.InsertRequestTopic, cfg.ReplyTopicPrefix)
//	if err != nil { log.Fatal(err) }
//	patch.Pub = pub
//	defer pub.Close()
var Pub *mqttpub.Publisher

// SendPatchRequest publishes a plain insert request to EMQX for the
// downstream insert engine (gokafka-raw) to pick up and write to Supabase.
// This is fire-and-forget — gopatch does not wait for the insert to
// complete and there is no reply/PLC write-back for this path.
//
// data should be the fully-shaped envelope (tenant_id, device_id, readings,
// output, limits, status, metric_a/b/c, energy) — mqttpub adds
// correlation_id/mode as sibling keys before publishing, no wrapping field.
func SendPatchRequest(data map[string]any) error {
	if Pub == nil {
		return fmt.Errorf("[patch] Pub is not initialized (call mqttpub.NewPublisher and set patch.Pub at startup)")
	}
	if err := Pub.Publish(data); err != nil {
		return fmt.Errorf("failed to publish patch request: %w", err)
	}
	return nil
}
