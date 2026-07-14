package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Application-wide configuration variables
var (
	APIUrl         string  // legacy: URL for the old PostgREST API endpoint (unused by the MQTT insert path, kept for reference/rollback)
	ServiceRoleKey string  // legacy: kept for reference/rollback
	Function       string  // legacy: kept for reference/rollback
	Trigger        string  // Trigger identifier for the device or operation
	LoopStr        string  // Looping parameter in string format
	Loop           float64 // Looping parameter converted to float64
	Filter         string  // Filter for processing MQTT messages
	InsertMode     string  // Default Mode : Patch, Option" Upsert

	Broker        string // Mosquitto broker hostname (subscriber)
	Port          string // Mosquitto broker port (subscriber)
	Topic         string // MQTT topic to subscribe to
	MQTTSStr      string // Indicates if MQTT Secure (MQTTS) is enabled ("true"/"false")
	ECScaCert     string // ESC version direct read from params store
	ECSclientCert string // ESC version direct read from params store
	ECSclientKey  string // ESC version direct read from params store

	// Insert/upsert publish path — this talks to EMQX, which is a
	// different broker from the Mosquitto instance Broker/Port above
	// point the subscriber at, so it gets its own connection settings.
	EMQXBroker         string // EMQX host, no scheme
	EMQXPort           string
	EMQXUsername       string
	EMQXPassword       string
	EMQXTLSStr         string // "true"/"false"
	EMQXCaCert         string // optional, PEM
	EMQXClientCert     string // optional, PEM (mTLS)
	EMQXClientKey      string // optional, PEM (mTLS)
	EMQXClientIDPrefix string // prepended to a fresh UUID per connection, e.g. to tell edge units apart in the EMQX dashboard
	MQTTDebug          bool   // verbose paho packet logging — noisy, off by default

	InsertRequestTopic string        // topic gopatch publishes patch/upsert requests to
	ReplyTopicPrefix   string        // reply topic prefix; full reply topic is prefix+correlation_id
	ReplyTimeoutSec    int           // how long to wait for an upsert reply before giving up
	ReplyTimeout       time.Duration // ReplyTimeoutSec as a time.Duration

	// ps-dashboard `readings` table identity — every row needs these two.
	TenantID string // ps-dashboard tenant UUID this edge device belongs to
	DeviceID string // ps-dashboard device UUID for this specific edge unit

	PlcHost         string // plcHost stores the PLC's hostname
	PlcPort         int    // plcPort stores the PLC's port number
	FxStr           string // Mitsubishi PLC FX series true =1 false =0
	PlcDevice       string // Mitsubishi PLC Device Number
	PlcData         string // Data register to PLC Device
	PlcDeviceUpsert string // Data register to PLC Device for Upsert

	// Cycle buffer
	CycleModeEnabled bool          // Enable cycle-boundary detection via anchor topic repeat
	CycleAnchorTopic string        // The very first MQTT topic the PLC publishes each scan cycle
	CycleTimeout     time.Duration // Safety flush if anchor topic goes silent (e.g. 5s)

	// Heartbeat — periodic liveness ping over the same insert/upsert
	// publish path (mqttpub.Publisher), landing in the same table as
	// readings rows with status.kind="heartbeat" and everything else null.
	HeartbeatIntervalSec int           // how often to publish a heartbeat
	HeartbeatInterval    time.Duration // HeartbeatIntervalSec as a time.Duration
	AppVersion           string        // reported as status.fw in the heartbeat payload
)

// --------------------------------------------------------------------------
// Config structs
// --------------------------------------------------------------------------

type MqttConfig struct {
	Broker        string
	Port          string
	Topic         string
	MQTTSStr      string
	ECScaCert     string
	ECSclientCert string
	ECSclientKey  string

	// Cycle buffer settings
	CycleModeEnabled bool
	CycleAnchorTopic string
	CycleTimeout     time.Duration
}

func GetMqttConfig() MqttConfig {
	return MqttConfig{
		Broker:        Broker,
		Port:          Port,
		Topic:         Topic,
		MQTTSStr:      MQTTSStr,
		ECScaCert:     ECScaCert,
		ECSclientCert: ECSclientCert,
		ECSclientKey:  ECSclientKey,
	}
}

// PublisherMqttConfig returns the EMQX connection settings needed to build
// the mqttpub.Publisher's mqtt.ClientOptions in main.go. This is a
// different broker than the subscriber's Mosquitto config (MqttConfig
// above) — pub goes to EMQX with username/password auth.
type PublisherMqttConfig struct {
	Broker         string
	Port           string
	Username       string
	Password       string
	ClientIDPrefix string
	UseTLS         bool
	CACert         string
	ClientCert     string
	ClientKey      string

	InsertRequestTopic string
	ReplyTopicPrefix   string
	ReplyTimeout       time.Duration
	MQTTDebug          bool
}

func GetPublisherMqttConfig() PublisherMqttConfig {
	useTLS, _ := strconv.ParseBool(EMQXTLSStr)
	return PublisherMqttConfig{
		Broker:         EMQXBroker,
		Port:           EMQXPort,
		Username:       EMQXUsername,
		Password:       EMQXPassword,
		ClientIDPrefix: EMQXClientIDPrefix,
		UseTLS:         useTLS,
		CACert:         EMQXCaCert,
		ClientCert:     EMQXClientCert,
		ClientKey:      EMQXClientKey,

		InsertRequestTopic: InsertRequestTopic,
		ReplyTopicPrefix:   ReplyTopicPrefix,
		ReplyTimeout:       ReplyTimeout,
		MQTTDebug:          MQTTDebug,
	}
}

type AppConfig struct {
	APIUrl         string // legacy, unused by MQTT insert path
	ServiceRoleKey string // legacy, unused by MQTT insert path
	Function       string // legacy, unused by MQTT insert path
	Trigger        string
	LoopStr        string
	Loop           float64
	Filter         string
	InsertMode     string

	InsertRequestTopic string
	ReplyTopicPrefix   string
	ReplyTimeout       time.Duration

	TenantID string
	DeviceID string

	Plc PlcConfig
}

func GetAppConfig() AppConfig {
	return AppConfig{
		APIUrl:         APIUrl,
		ServiceRoleKey: ServiceRoleKey,
		Function:       Function,
		Trigger:        Trigger,
		LoopStr:        LoopStr,
		Loop:           Loop,
		Filter:         Filter,
		InsertMode:     InsertMode,

		InsertRequestTopic: InsertRequestTopic,
		ReplyTopicPrefix:   ReplyTopicPrefix,
		ReplyTimeout:       ReplyTimeout,

		TenantID: TenantID,
		DeviceID: DeviceID,

		Plc: GetPlcConfig(),
	}
}

type PlcConfig struct {
	PlcHost         string // plcHost stores the PLC's hostname
	PlcPort         int    // plcPort stores the PLC's port number
	FxStr           string // Mitsubishi PLC FX series true =1 false =0
	PlcDevice       string
	PlcData         string
	PlcDeviceUpsert string
}

func GetPlcConfig() PlcConfig {
	return PlcConfig{
		PlcHost:         PlcHost,
		PlcPort:         PlcPort,
		FxStr:           FxStr,
		PlcDevice:       PlcDevice,
		PlcData:         PlcData,
		PlcDeviceUpsert: PlcDeviceUpsert,
	}
}

// HeartbeatConfig holds everything main.go needs to start the periodic
// heartbeat publisher. TenantID/DeviceID are shared with AppConfig — same
// identity, just also needed here since main.go wires the heartbeat up
// separately from handler.ProcessMQTTData.
type HeartbeatConfig struct {
	TenantID string
	DeviceID string
	Version  string
	Interval time.Duration
}

func GetHeartbeatConfig() HeartbeatConfig {
	return HeartbeatConfig{
		TenantID: TenantID,
		DeviceID: DeviceID,
		Version:  AppVersion,
		Interval: HeartbeatInterval,
	}
}

// --------------------------------------------------------------------------
// Load
// --------------------------------------------------------------------------

// Load initializes all configuration variables from environment variables
func Load(files ...string) {
	// Try to load from the specified file first
	if len(files) > 0 {
		for _, file := range files {
			err := godotenv.Load(file)
			if err != nil {
				log.Printf("Info: %s not found or failed to load local.env, falling back to system environment", file)
			}
		}
	}

	APIUrl = os.Getenv("API_URL")
	ServiceRoleKey = getEnv("SERVICE_ROLE_KEY", "")
	Function = getEnv("BASH_API", "")
	Trigger = getEnv("TRIGGER_DEVICE", "")
	Filter = getEnv("FILTER", "d174")
	InsertMode = os.Getenv("INSERT_MODE")

	LoopStr = getEnv("LOOPING", "1")
	Loop, _ = strconv.ParseFloat(LoopStr, 64)

	Broker = os.Getenv("MQTT_HOST")
	Port = getEnv("MQTT_PORT", "8883")
	Topic = os.Getenv("MQTT_TOPIC")
	MQTTSStr = getEnv("MQTTS_ON", "true")
	ECScaCert = os.Getenv("ECS_MQTT_CA_CERTIFICATE")
	ECSclientCert = os.Getenv("ECS_MQTT_CLIENT_CERTIFICATE")
	ECSclientKey = os.Getenv("ECS_MQTT_PRIVATE_KEY")

	// EMQX publisher connection — separate broker from the Mosquitto
	// subscriber above, so it gets its own host/port/auth/TLS.
	EMQXBroker = os.Getenv("EMQX_HOST")
	EMQXPort = getEnv("EMQX_PORT", "8883")
	EMQXUsername = os.Getenv("EMQX_USERNAME")
	EMQXPassword = os.Getenv("EMQX_PASSWORD")
	EMQXTLSStr = getEnv("EMQX_TLS_ON", "true")
	EMQXCaCert = os.Getenv("EMQX_CA_CERTIFICATE")
	EMQXClientCert = os.Getenv("EMQX_CLIENT_CERTIFICATE")
	EMQXClientKey = os.Getenv("EMQX_PRIVATE_KEY")
	EMQXClientIDPrefix = getEnv("EMQX_CLIENT_ID_PREFIX", "gopub-edge_publisher_")
	MQTTDebug, _ = strconv.ParseBool(getEnv("MQTT_DEBUG", "false"))

	// Insert/upsert publish path
	InsertRequestTopic = getEnv("MQTT_INSERT_REQUEST_TOPIC", "gopub-edge/devices/payload")
	ReplyTopicPrefix = getEnv("MQTT_REPLY_TOPIC_PREFIX", "gopub-edge/reply/")
	ReplyTimeoutSec, _ = strconv.Atoi(getEnv("MQTT_REPLY_TIMEOUT_SEC", "10"))
	ReplyTimeout = time.Duration(ReplyTimeoutSec) * time.Second

	// ps-dashboard `readings` table identity
	TenantID = os.Getenv("TENANT_ID")
	DeviceID = os.Getenv("DEVICE_ID")
	if TenantID == "" || DeviceID == "" {
		log.Println("⚠ TENANT_ID and/or DEVICE_ID not set — outgoing readings rows will be missing this identity until you set them")
	}

	PlcHost = os.Getenv("PLC_HOST")
	PlcPortStr := getEnv("PLC_PORT", "5011")
	PlcPort, _ = strconv.Atoi(PlcPortStr) // int for port
	FxStr = os.Getenv("PLC_MODEL")
	PlcDevice = os.Getenv("PLC_DEVICE")
	PlcData = os.Getenv("PLC_DATA")
	PlcDeviceUpsert = os.Getenv("PLC_DEVICE_UPSERT")

	// Cycle buffer
	CycleModeEnabled, _ = strconv.ParseBool(getEnv("CYCLE_MODE_ENABLED", "false"))
	CycleAnchorTopic = os.Getenv("CYCLE_ANCHOR_TOPIC")
	cycleTimeoutSec, _ := strconv.Atoi(getEnv("CYCLE_TIMEOUT_SEC", "5"))
	CycleTimeout = time.Duration(cycleTimeoutSec) * time.Second

	// Validate cycle config at startup so misconfiguration fails fast
	if CycleModeEnabled && CycleAnchorTopic == "" {
		log.Fatal("CYCLE_MODE_ENABLED=true but CYCLE_ANCHOR_TOPIC is not set")
	}

	// Heartbeat
	HeartbeatIntervalSec, _ = strconv.Atoi(getEnv("HEARTBEAT_INTERVAL_SEC", "30"))
	HeartbeatInterval = time.Duration(HeartbeatIntervalSec) * time.Second
	AppVersion = getEnv("APP_VERSION", "dev")

	// Fail fast on a missing EMQX host instead of the cryptic
	// "dial tcp :8883: connect: connection refused" you get from an empty
	// broker address reaching net.Dial.
	if EMQXBroker == "" {
		log.Fatal("EMQX_HOST is not set — check your .env.local is present and loaded, or export it in your shell")
	}

}

// Helper to get environment variable with fallback
// AWS ECS only allow os.Getenv
func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
