package mqtts

import (
	"testing"
	"time"

	"gopub-edge/model"
)

func TestAnchorMatchesDeviceName(t *testing.T) {
	cases := []struct {
		anchor, topic string
		want          bool
	}{
		{"x26", "holding_register/all/X26", true},
		{"X26", "holding_register/all/x26", true},
		{"holding_register/all/X26", "holding_register/all/X26", true},
		{"x26", "holding_register/all/X260", false},
		{"d1010", "holding_register/all/D392", false},
		{"holding_register/lot/X26", "holding_register/all/X26", false},
	}
	for _, c := range cases {
		cb := &CycleBuffer{firstTopic: c.anchor}
		if got := cb.matchesAnchor(c.topic); got != c.want {
			t.Errorf("anchor %q topic %q: got %v, want %v", c.anchor, c.topic, got, c.want)
		}
	}
}

// One PLC scan as msp-mcs publishes it: blocks in a fixed order, X26 once.
var scan = []string{"D0", "D14", "D392", "D418", "D676", "M28", "L90", "X26"}

func feedScan(cb *CycleBuffer) {
	for _, a := range scan {
		cb.Feed("holding_register/all/"+a, model.Message{Address: a, Value: 1})
	}
}

func TestCycleHoldsEveryDeviceOnce(t *testing.T) {
	cb := NewCycleBuffer("x26", 0)
	if cb.Active() {
		t.Fatal("active before the anchor was seen")
	}
	feedScan(cb)
	if !cb.Active() {
		t.Fatal("not active after the anchor")
	}
	feedScan(cb) // second X26 closes the first full cycle

	select {
	case cycle := <-cb.Cycles():
		got := map[string]int{}
		for _, m := range cycle {
			got[m.Address]++
		}
		for _, a := range scan {
			if got[a] != 1 {
				t.Errorf("%s appears %d times in the cycle, want 1 (cycle %v)", a, got[a], got)
			}
		}
	default:
		t.Fatal("no cycle emitted")
	}
}

func TestWrongAnchorStaysInactive(t *testing.T) {
	cb := NewCycleBuffer("d1010", 0)
	for i := 0; i < 5; i++ {
		feedScan(cb)
	}
	if cb.Active() {
		t.Fatal("active with an anchor that is never published")
	}
}

func TestAnchorSilentGoesInactive(t *testing.T) {
	cb := &CycleBuffer{firstTopic: "x26", timeout: 50 * time.Millisecond, cycleReady: make(chan []model.Message, 4)}
	feedScan(cb)
	if !cb.Active() {
		t.Fatal("not active after the anchor")
	}
	time.Sleep(60 * time.Millisecond)
	if cb.Active() {
		t.Fatal("still active after the anchor went silent past the timeout")
	}
}

// With cycle mode working, messages must not also go to the legacy queue.
func TestLegacyBypassedWhileCycleActive(t *testing.T) {
	ResetReceivedMessages()
	orig := globalCycleBuffer
	t.Cleanup(func() { globalCycleBuffer = orig; ResetReceivedMessages() })

	globalCycleBuffer = NewCycleBuffer("test_topic", 0) // mockMessage.Topic() == "test_topic"
	messageReceived(&mockMessage{payload: []byte(`{"address":"X26","value":0}`)})
	messageReceived(&mockMessage{payload: []byte(`{"address":"D392","value":1}`)})

	receivedMessagesMutex.Lock()
	n := len(receivedMessages)
	receivedMessagesMutex.Unlock()
	if n != 0 {
		t.Fatalf("legacy queue has %d messages while cycle mode is active, want 0", n)
	}
}

func TestLegacyUsedWhileAnchorUnseen(t *testing.T) {
	ResetReceivedMessages()
	orig := globalCycleBuffer
	t.Cleanup(func() { globalCycleBuffer = orig; ResetReceivedMessages() })

	globalCycleBuffer = NewCycleBuffer("d1010", 0)
	messageReceived(&mockMessage{payload: []byte(`{"address":"D392","value":1}`)})

	receivedMessagesMutex.Lock()
	n := len(receivedMessages)
	receivedMessagesMutex.Unlock()
	if n != 1 {
		t.Fatalf("legacy queue has %d messages with an unseen anchor, want 1", n)
	}
}
