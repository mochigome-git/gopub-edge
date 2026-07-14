package mqtts

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"gopub-edge/config"
	"gopub-edge/model"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type MqttData struct {
	Address string `json:"address"`
	Value   any    `json:"value"`
}

var (
	// Legacy burst-flusher state
	receivedMessages      []MqttData
	receivedMessagesMutex sync.Mutex
	droppedMessagesCount  int64
	lastMessageTime       time.Time
	burstActive           bool
	messageCount          int
	lastMessageCount      int

	// Cycle-buffer instance (nil when CycleModeEnabled = false)
	globalCycleBuffer *CycleBuffer
)

const (
	// Legacy burst-flusher tuning
	MinFlushSize  = 200
	MaxQueueSize  = 500
	FlushInterval = 2 * time.Second

	// Burst detection
	BurstDetectionGap = 150 * time.Millisecond
	MessageStability  = 300 * time.Millisecond
	CheckInterval     = 50 * time.Millisecond
)

// --------------------------------------------------------------------------
// TLS / connection options
// --------------------------------------------------------------------------

func getClientOptions(broker, port string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%s", broker, port))
	clientID := "go_mqtt_subscriber_" + uuid.New().String()
	opts.SetClientID(clientID)
	opts.SetUsername("emqx")
	opts.SetPassword("public")
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	return opts
}

func ECSgetClientOptionsTLS(broker, port, ECScaCert, ECSclientCert, ECSclientKey string) (*mqtt.ClientOptions, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtts://%s:%s", broker, port))
	clientID := "go_mqtt_subscriber_" + uuid.New().String()

	cert, err := tls.X509KeyPair([]byte(ECSclientCert), []byte(ECSclientKey))
	if err != nil {
		return nil, fmt.Errorf("error loading client certificate/key: %s", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(ECScaCert)) {
		return nil, fmt.Errorf("failed to append CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}

	opts.SetClientID(clientID)
	opts.SetUsername("emqx")
	opts.SetPassword("public")
	opts.SetTLSConfig(tlsConfig)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	return opts, nil
}

// --------------------------------------------------------------------------
// Connection (split out of the old Client() so the connected client can be
// reused elsewhere — e.g. mqttpub.Publisher subscribing for replies on this
// same broker instead of opening a second connection)
// --------------------------------------------------------------------------

// Connect builds client options from cfg (TLS or plain, matching MQTTSStr)
// and connects, retrying up to 5 times with a 2s backoff. Returns the
// connected client — caller is responsible for eventually calling Run (to
// subscribe cfg.Topic and block until shutdown) and/or Disconnect.
func Connect(cfg config.MqttConfig) (mqtt.Client, error) {
	mqtts, _ := strconv.ParseBool(cfg.MQTTSStr)
	var opts *mqtt.ClientOptions

	if mqtts {
		var err error
		opts, err = ECSgetClientOptionsTLS(cfg.Broker, cfg.Port, cfg.ECScaCert, cfg.ECSclientCert, cfg.ECSclientKey)
		if err != nil {
			return nil, fmt.Errorf("error requesting MQTT TLS configuration: %w", err)
		}
	} else {
		opts = getClientOptions(cfg.Broker, cfg.Port)
	}

	client := mqtt.NewClient(opts)

	maxAttempts := 5
	var lastErr error
	for i := 1; i <= maxAttempts; i++ {
		if token := client.Connect(); token.Wait() && token.Error() == nil {
			return client, nil
		} else {
			lastErr = token.Error()
			log.Printf("MQTT connect failed (attempt %d/%d): %v", i, maxAttempts, lastErr)
			if i < maxAttempts {
				time.Sleep(2 * time.Second)
			}
		}
	}
	return nil, fmt.Errorf("MQTT connect failed after %d attempts: %w", maxAttempts, lastErr)
}

// Run subscribes an already-connected client (see Connect) to cfg.Topic,
// starts the cycle buffer + legacy flusher, and blocks until
// SIGINT/SIGTERM, then shuts down gracefully (unsubscribes, disconnects,
// closes clientDone). Any other subscriptions added to client before
// calling Run (e.g. a reply-topic listener sharing this connection)
// continue running independently — this only manages cfg.Topic's lifecycle.
func Run(client mqtt.Client, cfg config.MqttConfig, receivedMessagesJSONChan chan<- string, clientDone chan<- struct{}) {
	// -----------------------------------------------------------------
	// Cycle buffer setup
	// All config comes from cfg (populated by config.Load from .env / ECS).
	// Relevant env vars:
	//   CYCLE_MODE_ENABLED=true
	//   CYCLE_ANCHOR_TOPIC=plc/d100   ← first topic PLC publishes each scan
	//   CYCLE_TIMEOUT_SEC=5           ← safety flush if anchor goes silent
	// -----------------------------------------------------------------
	stopCycle := make(chan struct{})

	if cfg.CycleModeEnabled {
		globalCycleBuffer = NewCycleBuffer(cfg.CycleAnchorTopic, cfg.CycleTimeout)
		go consumeCycles(globalCycleBuffer, receivedMessagesJSONChan, stopCycle)
		log.Printf("✓ Cycle mode enabled — anchor: %s, timeout: %v", cfg.CycleAnchorTopic, cfg.CycleTimeout)
	}

	// -----------------------------------------------------------------
	// Legacy burst flusher (runs alongside cycle mode during migration).
	// Remove startBatchFlusher and the legacy block in messageReceived
	// once cycle mode is fully validated (Option B cutover).
	// -----------------------------------------------------------------
	stopFlusher := make(chan struct{})
	go startBatchFlusher(receivedMessagesJSONChan, stopFlusher)

	if token := client.Subscribe(cfg.Topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		messageReceived(msg)
	}); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to topic: %v", token.Error())
		return
	}

	log.Printf("Subscribed to topic: %s\n", cfg.Topic)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	if cfg.CycleModeEnabled && globalCycleBuffer != nil {
		globalCycleBuffer.Stop()
		close(stopCycle)
	}
	close(stopFlusher)
	client.Unsubscribe(cfg.Topic)
	client.Disconnect(250)
	close(clientDone)
	log.Println("MQTT client shut down gracefully.")
}

// Client connects and runs, blocking until shutdown — kept for backward
// compatibility with existing callers. Equivalent to Connect(cfg) followed
// by Run(client, cfg, ...). New code that needs the connected client for
// additional subscriptions (e.g. reply listening) should call Connect and
// Run separately instead, the way main.go does.
func Client(cfg config.MqttConfig, receivedMessagesJSONChan chan<- string, clientDone chan<- struct{}) {
	client, err := Connect(cfg)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	Run(client, cfg, receivedMessagesJSONChan, clientDone)
}

// --------------------------------------------------------------------------
// Message ingestion
// --------------------------------------------------------------------------

// messageReceived handles every incoming MQTT message.
// It feeds both the cycle buffer (when enabled) and the legacy burst flusher.
func messageReceived(msg mqtt.Message) {
	var mqttData MqttData
	if err := json.Unmarshal(msg.Payload(), &mqttData); err != nil {
		log.Printf("Error parsing JSON: %v\n", err)
		return
	}

	// --- Cycle buffer path ---
	// Uses the raw MQTT topic as the cycle-boundary signal.
	// Active only when CYCLE_MODE_ENABLED=true.
	if globalCycleBuffer != nil {
		globalCycleBuffer.Feed(msg.Topic(), model.Message{
			Address: mqttData.Address,
			Value:   mqttData.Value,
		})
	}

	// --- Legacy burst flusher path ---
	// Kept for parallel operation during migration.
	receivedMessagesMutex.Lock()
	now := time.Now()
	if lastMessageTime.IsZero() || now.Sub(lastMessageTime) < BurstDetectionGap {
		burstActive = true
	}
	receivedMessages = append(receivedMessages, mqttData)
	messageCount++
	lastMessageTime = now
	receivedMessagesMutex.Unlock()
}

// --------------------------------------------------------------------------
// Cycle consumer
// --------------------------------------------------------------------------

// consumeCycles reads complete PLC scan cycles from the CycleBuffer and
// dispatches them as JSON on receivedMessagesJSONChan.
//
// Each cycle emission is guaranteed to contain every D-device message from
// one full PLC scan, so multi-register strings (model_name, ink_lot, etc.)
// are always assembled from complete data before downstream handlers run.
func consumeCycles(cb *CycleBuffer, receivedMessagesJSONChan chan<- string, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return

		case cycle, ok := <-cb.Cycles():
			if !ok || len(cycle) == 0 {
				continue
			}

			mqttDataSlice := make([]MqttData, len(cycle))
			for i, m := range cycle {
				mqttDataSlice[i] = MqttData{
					Address: m.Address,
					Value:   m.Value,
				}
			}

			jsonData, err := json.Marshal(mqttDataSlice)
			if err != nil {
				log.Printf("[CycleConsumer] Error marshaling cycle JSON: %v", err)
				continue
			}

			select {
			case receivedMessagesJSONChan <- string(jsonData):
				log.Printf("[CycleConsumer] ✓ Dispatched cycle: %d messages", len(cycle))
			default:
				atomic.AddInt64(&droppedMessagesCount, 1)
				log.Printf("[CycleConsumer] ⚠ Cycle dropped — channel full (%d messages lost)", len(cycle))
			}
		}
	}
}

// --------------------------------------------------------------------------
// Legacy burst flusher (unchanged from original)
// --------------------------------------------------------------------------

func startBatchFlusher(receivedMessagesJSONChan chan<- string, stopFlusher <-chan struct{}) {
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	checkTicker := time.NewTicker(CheckInterval)
	defer checkTicker.Stop()

	for {
		select {
		case <-ticker.C:
			flushMessages(receivedMessagesJSONChan, true)
		case <-checkTicker.C:
			flushMessages(receivedMessagesJSONChan, false)
		case <-stopFlusher:
			flushMessages(receivedMessagesJSONChan, true)
			return
		}
	}
}

func flushMessages(receivedMessagesJSONChan chan<- string, force bool) {
	receivedMessagesMutex.Lock()
	defer receivedMessagesMutex.Unlock()

	queueLen := len(receivedMessages)
	if queueLen == 0 {
		return
	}

	timeSinceLastMsg := time.Since(lastMessageTime)
	isStable := messageCount == lastMessageCount
	lastMessageCount = messageCount

	burstJustEnded := burstActive && timeSinceLastMsg >= BurstDetectionGap
	if burstJustEnded {
		burstActive = false
		log.Printf("[BurstFlusher] 📦 Burst ended, flushing %d messages (gap: %v)", queueLen, timeSinceLastMsg)
	}

	shouldFlush := force ||
		(float64(queueLen) >= float64(MaxQueueSize)*0.8) ||
		burstJustEnded ||
		(queueLen >= MinFlushSize && isStable && timeSinceLastMsg >= MessageStability)

	if shouldFlush {
		messagesToSend := receivedMessages
		receivedMessages = nil
		messageCount = 0

		jsonData, err := json.Marshal(messagesToSend)
		if err != nil {
			log.Printf("[BurstFlusher] Error marshaling JSON: %v\n", err)
			return
		}

		select {
		case receivedMessagesJSONChan <- string(jsonData):
		default:
			atomic.AddInt64(&droppedMessagesCount, 1)
		}
	}
}

// --------------------------------------------------------------------------
// Utility
// --------------------------------------------------------------------------

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	log.Println("✓ Connected to MQTT broker")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Fatalf("✗ Connection lost: %v\n", err)
}

func ResetReceivedMessages() {
	receivedMessagesMutex.Lock()
	receivedMessages = []MqttData{}
	messageCount = 0
	lastMessageCount = 0
	burstActive = false
	lastMessageTime = time.Time{}
	receivedMessagesMutex.Unlock()
	log.Println("🔄 Received messages buffer reset")
}

// GetBufferStats returns combined diagnostics for both the legacy flusher and cycle buffer.
func GetBufferStats() map[string]any {
	receivedMessagesMutex.Lock()
	legacyStats := map[string]any{
		"queue_length":     len(receivedMessages),
		"burst_active":     burstActive,
		"time_since_last":  time.Since(lastMessageTime).Milliseconds(),
		"dropped_messages": atomic.LoadInt64(&droppedMessagesCount),
		"message_count":    messageCount,
	}
	receivedMessagesMutex.Unlock()

	stats := map[string]any{
		"legacy": legacyStats,
	}

	if globalCycleBuffer != nil {
		stats["cycle"] = globalCycleBuffer.Stats()
	}

	return stats
}
