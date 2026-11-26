package mqtts

import (
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestReconnectRestoresSubscriptions(t *testing.T) {
	everConnected.Store(false)
	resubMu.Lock()
	saved := resubs
	resubs = nil
	resubMu.Unlock()
	t.Cleanup(func() { resubMu.Lock(); resubs = saved; resubMu.Unlock() })

	calls := 0
	OnReconnect(func(mqtt.Client) { calls++ })

	connectHandler(nil) // first connect
	if calls != 0 {
		t.Fatalf("hooks ran on the first connect (%d), want 0", calls)
	}
	connectHandler(nil) // reconnect
	connectHandler(nil) // reconnect again
	if calls != 2 {
		t.Fatalf("hooks ran %d times over 2 reconnects, want 2", calls)
	}
}

func TestConnectionLostDoesNotExit(t *testing.T) {
	// was log.Fatalf — reaching the next line proves it no longer exits
	connectLostHandler(nil, errTest{})
}

type errTest struct{}

func (errTest) Error() string { return "test" }
