// Package mqttpub replaces the direct-to-Supabase HTTP patch/upsert calls
// with MQTT publishes to EMQX. A separate downstream engine (gokafka-raw,
// or whatever you point at the request topic) does the actual insert.
//
// Two usage modes:
//
//   - Publish(mode, data): fire-and-forget. No reply_topic is attached, so
//     the downstream engine knows not to bother replying. Used for the
//     plain "patch" path where nothing in gopatch depends on the result.
//
//   - PublishAndAwaitReply(ctx, mode, data, timeout): publishes with a
//     fresh correlation_id and a reply_topic (replyPrefix + correlation_id),
//     then blocks on an internal channel until a reply lands on that topic,
//     the timeout elapses, or ctx is cancelled. Used for "upsert", where
//     gopatch needs the returned row back to write PLC devices.
package mqttpub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// ReplyPayload is what the downstream insert engine publishes back on the
// per-request reply topic once it has finished the insert/upsert.
type ReplyPayload struct {
	CorrelationID string          `json:"correlation_id"`
	Success       bool            `json:"success"`
	Error         string          `json:"error,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"` // raw row(s), same shape the old PostgREST response had
}

// RequestEnvelope is what gopatch publishes to the insert-request topic.
type RequestEnvelope struct {
	CorrelationID string          `json:"correlation_id"`
	ReplyTopic    string          `json:"reply_topic,omitempty"` // empty => fire-and-forget, do not reply
	Mode          string          `json:"mode"`                  // "patch" | "upsert"
	Data          json.RawMessage `json:"data"`
}

// Publisher owns a single MQTT client used both for publishing
// insert/upsert requests and for receiving correlated replies on a
// wildcard reply-topic subscription.
type Publisher struct {
	client       mqtt.Client
	requestTopic string // e.g. "gopatch/insert/request"
	replyPrefix  string // e.g. "gopatch/reply/" — replies arrive on replyPrefix+correlation_id

	mu      sync.Mutex
	pending map[string]chan ReplyPayload
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

func (p *Publisher) handleReply(_ mqtt.Client, msg mqtt.Message) {
	var reply ReplyPayload
	if err := json.Unmarshal(msg.Payload(), &reply); err != nil {
		log.Printf("[mqttpub] ⚠ failed to parse reply on %s: %v", msg.Topic(), err)
		return
	}
	if reply.CorrelationID == "" {
		log.Printf("[mqttpub] ⚠ reply on %s missing correlation_id, dropping", msg.Topic())
		return
	}

	p.mu.Lock()
	ch, ok := p.pending[reply.CorrelationID]
	p.mu.Unlock()

	if !ok {
		// Nobody waiting anymore — most likely we already timed out.
		log.Printf("[mqttpub] reply for %s arrived after timeout (or unknown), dropping", reply.CorrelationID)
		return
	}

	select {
	case ch <- reply:
	default:
		// Channel is buffered size 1, this shouldn't happen, but never block the MQTT callback.
	}
}

// PublishAndAwaitReply publishes a request with a fresh correlation ID and
// blocks until a matching reply arrives, ctx is cancelled, or timeout elapses.
func (p *Publisher) PublishAndAwaitReply(ctx context.Context, mode string, data json.RawMessage, timeout time.Duration) (ReplyPayload, error) {
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

	envelope := RequestEnvelope{
		CorrelationID: correlationID,
		ReplyTopic:    replyTopic,
		Mode:          mode,
		Data:          data,
	}
	payload, err := json.Marshal(envelope)
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
		return ReplyPayload{}, fmt.Errorf("mqttpub: timed out after %v waiting for reply (correlation_id=%s)", timeout, correlationID)
	}
}

// Publish is fire-and-forget: no reply_topic is attached, so the
// downstream insert engine knows not to reply. Used for the plain
// "patch" path where nothing in gopatch needs the result back.
func (p *Publisher) Publish(mode string, data json.RawMessage) error {
	envelope := RequestEnvelope{
		CorrelationID: uuid.New().String(),
		Mode:          mode,
		Data:          data,
	}
	payload, err := json.Marshal(envelope)
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
