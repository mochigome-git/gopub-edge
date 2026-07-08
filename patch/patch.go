package patch

import (
	"fmt"

	"gopub-edge/internal/mqttpub"
)

// Pub is the shared MQTT publisher used by SendPatchRequest and
// SendUpsertRequest. Set this once at startup, e.g. in main.go:
//
//	opts := mqtts.ECSgetClientOptionsTLS(...)   // reuse broker/TLS config, distinct ClientID
//	pub, err := mqttpub.NewPublisher(opts, cfg.InsertRequestTopic, cfg.ReplyTopicPrefix)
//	if err != nil { log.Fatal(err) }
//	patch.Pub = pub
//	defer pub.Close()
var Pub *mqttpub.Publisher

// SendPatchRequest publishes a plain insert request to EMQX for the
// downstream insert engine (gokafka-raw) to pick up and write to Supabase.
// This is fire-and-forget — gopatch does not wait for the insert to
// complete and there is no reply/PLC write-back for this path.
func SendPatchRequest(jsonPayload []byte) error {
	if Pub == nil {
		return fmt.Errorf("patch: Pub is not initialized (call mqttpub.NewPublisher and set patch.Pub at startup)")
	}
	if err := Pub.Publish("patch", jsonPayload); err != nil {
		return fmt.Errorf("failed to publish patch request: %w", err)
	}
	return nil
}
