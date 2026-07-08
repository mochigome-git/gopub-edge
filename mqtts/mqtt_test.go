package mqtts

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Mock MQTT Message
type mockMessage struct {
	payload []byte
}

func (m *mockMessage) Duplicate() bool   { return false }
func (m *mockMessage) Qos() byte         { return 0 }
func (m *mockMessage) Retained() bool    { return false }
func (m *mockMessage) Topic() string     { return "test_topic" }
func (m *mockMessage) MessageID() uint16 { return 0 }
func (m *mockMessage) Payload() []byte   { return m.payload }
func (m *mockMessage) Ack()              {}

func TestMessageReceivedAndFlush(t *testing.T) {
	receivedMessagesJSONChan := make(chan string, 10) // buffered to avoid blocking
	stopFlusher := make(chan struct{})

	ResetReceivedMessages()

	// Start flusher in background
	go startBatchFlusher(receivedMessagesJSONChan, stopFlusher)

	// Fill up the queue
	for i := 0; i < MaxQueueSize; i++ {
		payload := fmt.Sprintf(`{"address":"address_%d","value":"value_%d"}`, i, i)
		testMessage := &mockMessage{payload: []byte(payload)}
		messageReceived(testMessage)
	}

	// Wait for flush
	var jsonOutput string
	select {
	case jsonOutput = <-receivedMessagesJSONChan:
		// success
	case <-time.After(2 * time.Second):
		close(stopFlusher)
		t.Fatal("Timed out waiting for flushed messages")
	}

	// Close flusher after receiving data
	close(stopFlusher)

	var messages []MqttData
	err := json.Unmarshal([]byte(jsonOutput), &messages)
	assert.NoError(t, err, "JSON should unmarshal correctly")
	assert.Len(t, messages, MaxQueueSize, fmt.Sprintf("Should contain %d messages", MaxQueueSize))
	assert.Equal(t, "address_0", messages[0].Address)
	assert.Equal(t, "value_0", messages[0].Value)
	assert.Equal(t, fmt.Sprintf("address_%d", MaxQueueSize-1), messages[MaxQueueSize-1].Address)
}

func TestResetReceivedMessages(t *testing.T) {
	// Pre-fill some fake messages
	for i := 0; i < 10; i++ {
		receivedMessages = append(receivedMessages, MqttData{
			Address: fmt.Sprintf("address_%d", i),
			Value:   fmt.Sprintf("value_%d", i),
		})
	}
	assert.NotEmpty(t, receivedMessages, "Message queue should be prefilled")

	ResetReceivedMessages()

	assert.Empty(t, receivedMessages, "Message queue should be empty after ResetReceivedMessages")
}
