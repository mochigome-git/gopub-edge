// Package mqttpub replaces the direct-to-Supabase HTTP patch/upsert calls
// with MQTT publishes. A separate downstream engine (gokafka-raw, or
// whatever you point at the request topic) does the actual insert.
//
// Requests go out over one broker (typically EMQX, the request-publishing
// side) and replies come back over a second, potentially different broker
// (typically the local edge Mosquitto gopub-edge already has a connection
// to for PLC data) — NewPublisher takes an already-connected client for
// the reply side rather than building its own, so that connection can be
// shared with whatever else is using it instead of opening a redundant one.
//
// Three usage modes:
//
//   - Publish(data): fire-and-forget. No reply_topic is attached, so the
//     downstream engine knows not to bother replying. Used for the plain
//     "patch" path where nothing in gopub-edge depends on the result.
//
//   - PublishAndAwaitReply(ctx, data, timeout): publishes with a reply_topic
//     (replyPrefix + a fresh internal correlation id) over the REMOTE
//     broker (pubClient, EMQX), then blocks until a message arrives on
//     that exact topic (via the reply-side client), the timeout elapses,
//     or ctx is cancelled. Used for "upsert", where gopub-edge needs the
//     returned row back to write PLC devices.
//
//   - PublishAndAwaitReplyLocal(ctx, localTopic, data, timeout): same
//     request/reply shape as above, but the REQUEST itself goes out over
//     the LOCAL broker (replyClient, Mosquitto) instead of EMQX — for
//     consumers running on the same edge unit (e.g. vacuum-engine), where
//     round-tripping the request through the cloud broker just to come
//     back down to the same LAN is pure added latency for no benefit.
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

// Publisher owns two MQTT client connections: pubClient publishes
// insert/upsert requests (typically EMQX), replyClient subscribes to a
// wildcard reply-topic to receive correlated replies (typically the local
// edge Mosquitto). They can be the same broker if you build both
// mqtt.ClientOptions pointing at it.
type Publisher struct {
	pubClient   mqtt.Client
	replyClient mqtt.Client

	requestTopic string // e.g. "gopub-edge/insert/request" — published via pubClient
	replyPrefix  string // e.g. "gopub-edge/reply/" — replies arrive here via replyClient, keyed by replyPrefix+correlation_id

	mu      sync.Mutex
	pending map[string]chan ReplyPayload // keyed by correlation id, extracted from the reply's MQTT topic
}

// NewPublisher connects pubOpts (the request-publishing broker, e.g. EMQX)
// and subscribes replyClient — an ALREADY-CONNECTED client, typically the
// same Mosquitto connection gopub-edge already uses for PLC data — to
// replyPrefix+"#". Publisher does not own replyClient's lifecycle: Close()
// only disconnects the publish side; disconnecting replyClient is the
// caller's responsibility (it may be shared with other subscriptions).
func NewPublisher(pubOpts *mqtt.ClientOptions, replyClient mqtt.Client, requestTopic, replyPrefix string) (*Publisher, error) {
	p := &Publisher{
		requestTopic: requestTopic,
		replyPrefix:  replyPrefix,
		pending:      make(map[string]chan ReplyPayload),
	}

	pubClient := mqtt.NewClient(pubOpts)
	if token := pubClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqttpub: publish client connect failed: %w", token.Error())
	}
	p.pubClient = pubClient
	p.replyClient = replyClient

	subTopic := replyPrefix + "#"
	if token := replyClient.Subscribe(subTopic, 1, p.handleReply); token.Wait() && token.Error() != nil {
		pubClient.Disconnect(250)
		return nil, fmt.Errorf("mqttpub: subscribe to %s failed: %w", subTopic, token.Error())
	}
	log.Printf("[mqttpub] ✓ connected — publishing requests to %q, listening for replies on %q", requestTopic, subTopic)

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

// PublishAndAwaitReply publishes data (via pubClient, the REMOTE broker —
// typically EMQX) with a reply_topic attached and blocks until a message
// arrives on that topic (via replyClient), ctx is cancelled, or timeout
// elapses. The correlation id is internal bookkeeping only (used to build
// the reply topic and the pending-map key) — it is never put in the
// outgoing JSON.
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

	if token := p.pubClient.Publish(p.requestTopic, 1, false, payload); token.Wait() && token.Error() != nil {
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

// PublishAndAwaitReplyLocal is identical to PublishAndAwaitReply except the
// REQUEST itself goes out over the LOCAL broker (replyClient, typically
// Mosquitto) instead of the remote request broker (pubClient, typically
// EMQX). Use this when the consumer (e.g. vacuum-engine) runs on the same
// edge unit as gopub-edge — there's no reason to round-trip the request
// through the cloud broker just to come back down to the same LAN. The
// consumer is responsible for publishing its own finished result to EMQX
// separately, once it has one (see govacuum-engine-gc's
// mqtt.ConnectEMQXPublisher/PublishMetric).
func (p *Publisher) PublishAndAwaitReplyLocal(ctx context.Context, localTopic string, data map[string]any, timeout time.Duration) (ReplyPayload, error) {
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

	if token := p.replyClient.Publish(localTopic, 1, false, payload); token.Wait() && token.Error() != nil {
		return ReplyPayload{}, fmt.Errorf("mqttpub: local publish failed: %w", token.Error())
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
	if token := p.pubClient.Publish(p.requestTopic, 1, false, payload); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqttpub: publish failed: %w", token.Error())
	}
	return nil
}

// Close disconnects the request-publishing client only. The reply-listen
// client (replyClient passed into NewPublisher) is not owned by Publisher
// and is not disconnected here — its lifecycle belongs to whoever created
// it (e.g. gopub-edge's main.go, which shares it with the PLC subscriber).
func (p *Publisher) Close() {
	if p.pubClient != nil && p.pubClient.IsConnected() {
		p.pubClient.Disconnect(250)
	}
}
