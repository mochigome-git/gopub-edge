// Package mqttpub replaces the direct-to-Supabase HTTP patch/upsert calls
// with MQTT publishes to EMQX. A separate downstream engine (gokafka-raw,
// or whatever you point at the request topic) does the actual insert.
//
// Two usage modes:
//
//   - Publish(data): fire-and-forget. No reply_topic is attached, so the
//     downstream engine knows not to bother replying. Used for the plain
//     "patch" path where nothing in gopub-edge depends on the result.
//
//   - PublishAndAwaitReply(ctx, data, timeout): publishes with a reply_topic
//     (replyPrefix + a fresh internal correlation id), then blocks until a
//     message arrives on that exact topic, the timeout elapses, or ctx is
//     cancelled. Used for "upsert", where gopub-edge needs the returned row
//     back to write PLC devices.
//
// The published payload is a single flat JSON object: whatever's in data
// (tenant_id, readings, output, limits, status, ...) plus reply_topic when
// a reply is expected. There's no correlation_id or mode field in the
// payload — reply_topic's presence/absence is the patch-vs-upsert signal,
// and replies are matched by the MQTT topic they arrive on (which already
// encodes the correlation id), not by a field inside the message.
package mqttpub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// ReplyPayload is what the downstream insert engine publishes back on the
// per-request reply topic once it has finished the insert/upsert.
type ReplyPayload struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"` // raw row(s), same shape the old PostgREST response had
}

// Publisher owns a single MQTT client used both for publishing
// insert/upsert requests and for receiving correlated replies on a
// wildcard reply-topic subscription.
type Publisher struct {
	client       mqtt.Client
	requestTopic string // e.g. "gopub-edge/insert/request"
	replyPrefix  string // e.g. "gopub-edge/reply/" — replies arrive on replyPrefix+correlation_id

	mu      sync.Mutex
	pending map[string]chan ReplyPayload // keyed by correlation id, extracted from the reply's MQTT topic
}

// NewPublisher connects a dedicated MQTT client (pass in your own
// mqtt.ClientOptions — reuse the same TLS/broker config as your subscriber,
// just give it a distinct ClientID) and subscribes to replyPrefix+"#".
func NewPublisher(opts *mqtt.ClientOptions, requestTopic, replyPrefix string) (*Publisher, error) {
	p := &Publisher{
		requestTopic: requestTopic,
		replyPrefix:  replyPrefix,
		pending:      make(map[string]chan ReplyPayload),
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqttpub: connect failed: %w", token.Error())
	}
	p.client = client

	subTopic := replyPrefix + "#"
	if token := client.Subscribe(subTopic, 1, p.handleReply); token.Wait() && token.Error() != nil {
		client.Disconnect(250)
		return nil, fmt.Errorf("mqttpub: subscribe to %s failed: %w", subTopic, token.Error())
	}
	log.Printf("[mqttpub] ✓ connected — publishing to %q, replies on %q", requestTopic, subTopic)

	return p, nil
}

// handleReply matches a reply to its waiting caller by MQTT topic, not by
// any field inside the message body — the reply topic itself
// (replyPrefix+correlationID) is the correlation key.
func (p *Publisher) handleReply(_ mqtt.Client, msg mqtt.Message) {
	correlationID := strings.TrimPrefix(msg.Topic(), p.replyPrefix)
	if correlationID == "" || correlationID == msg.Topic() {
		log.Printf("[mqttpub] ⚠ reply on unexpected topic %q, dropping", msg.Topic())
		return
	}

	var reply ReplyPayload
	if err := json.Unmarshal(msg.Payload(), &reply); err != nil {
		log.Printf("[mqttpub] ⚠ failed to parse reply on %s: %v", msg.Topic(), err)
		return
	}

	p.mu.Lock()
	ch, ok := p.pending[correlationID]
	p.mu.Unlock()

	if !ok {
		// Nobody waiting anymore — most likely we already timed out.
		log.Printf("[mqttpub] reply for %s arrived after timeout (or unknown), dropping", correlationID)
		return
	}

	select {
	case ch <- reply:
	default:
		// Channel is buffered size 1, this shouldn't happen, but never block the MQTT callback.
	}
}

// flattenEnvelope returns a shallow copy of data with reply_topic merged in
// as a sibling key when replyTopic is non-empty. No wrapping field, no
// correlation_id/mode in the payload.
func flattenEnvelope(data map[string]any, replyTopic string) map[string]any {
	out := make(map[string]any, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	if replyTopic != "" {
		out["reply_topic"] = replyTopic
	}
	return out
}

// PublishAndAwaitReply publishes data with a reply_topic attached and
// blocks until a message arrives on that topic, ctx is cancelled, or
// timeout elapses. The correlation id is internal bookkeeping only (used
// to build the reply topic and the pending-map key) — it is never put in
// the outgoing JSON.
func (p *Publisher) PublishAndAwaitReply(ctx context.Context, data map[string]any, timeout time.Duration) (ReplyPayload, error) {
	correlationID := uuid.New().String()
	replyTopic := p.replyPrefix + correlationID

	replyCh := make(chan ReplyPayload, 1)
	p.mu.Lock()
	p.pending[correlationID] = replyCh
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pending, correlationID)
		p.mu.Unlock()
	}()

	payload, err := json.Marshal(flattenEnvelope(data, replyTopic))
	if err != nil {
		return ReplyPayload{}, fmt.Errorf("mqttpub: marshal request: %w", err)
	}

	if token := p.client.Publish(p.requestTopic, 1, false, payload); token.Wait() && token.Error() != nil {
		return ReplyPayload{}, fmt.Errorf("mqttpub: publish failed: %w", token.Error())
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-waitCtx.Done():
		return ReplyPayload{}, fmt.Errorf("mqttpub: timed out after %v waiting for reply (reply_topic=%s)", timeout, replyTopic)
	}
}

// Publish is fire-and-forget: no reply_topic is attached, so the
// downstream insert engine knows not to reply. Used for the plain
// "patch" path where nothing in gopub-edge needs the result back.
func (p *Publisher) Publish(data map[string]any) error {
	payload, err := json.Marshal(flattenEnvelope(data, ""))
	if err != nil {
		return fmt.Errorf("mqttpub: marshal request: %w", err)
	}
	if token := p.client.Publish(p.requestTopic, 1, false, payload); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqttpub: publish failed: %w", token.Error())
	}
	return nil
}

func (p *Publisher) Close() {
	if p.client != nil && p.client.IsConnected() {
		p.client.Disconnect(250)
	}
}
