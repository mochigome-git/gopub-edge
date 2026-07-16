package main

import (
	//"log"
	//"net/http"
	//_ "net/http/pprof"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopub-edge/config"
	"gopub-edge/handler"
	"gopub-edge/internal/app"
	"gopub-edge/internal/mqttpub"
	"gopub-edge/mqtts"
	"gopub-edge/patch"
)

func main() {
	// startTime is captured here, at process boot, so the heartbeat's
	// uptime_s reflects how long this gopub-edge process has been
	// running (matching how the ESP32 side reports its own uptime).
	startTime := time.Now()

	// Register the profiling handlers with the default HTTP server mux.
	// This will serve the profiling endpoints at /debug/pprof.
	/**
	Memory profile: http://localhost:6060/debug/pprof/heap
	Goroutine profile: http://localhost:6060/debug/pprof/goroutine
	CPU profile: http://localhost:6060/debug/pprof/profile
	**/
	// Start profiling server
	//go func() {
	//  if err := http.ListenAndServe("192.168.0.126:6060", nil); err != nil {
	//      log.Fatalf("Error starting profiling server: %v", err)
	//  }
	//}()

	// Load configuration — tries .env.local first, then .env for anything
	// .env.local didn't set, then finally falls back to real system env
	// vars for anything neither file set.
	config.Load(".env.local", ".env")

	logger := log.New(os.Stdout, "[PLC] ", log.LstdFlags|log.Lmsgprefix)

	// Create the Application once at startup
	plcApp, err := app.NewApplication(config.GetPlcConfig(), logger)
	if err != nil {
		logger.Fatalf("Failed to init PLC Application: %v", err)
	}
	defer plcApp.Close()

	// --- EMQX publisher (insert/upsert requests + correlated replies) ---
	// Distinct broker from the Mosquitto subscriber below — patch.SendPatchRequest
	// and patch.SendUpsertRequest use this instead of hitting Supabase directly.
	pubCfg := config.GetPublisherMqttConfig()

	if pubCfg.MQTTDebug {
		mqttpub.EnableDebugLogging()
	}

	emqxOpts := mqttpub.EMQXOptions{
		Broker:         pubCfg.Broker,
		Port:           pubCfg.Port,
		Username:       pubCfg.Username,
		Password:       pubCfg.Password,
		ClientIDPrefix: pubCfg.ClientIDPrefix,
		UseTLS:         pubCfg.UseTLS,
		CACert:         pubCfg.CACert,
		ClientCert:     pubCfg.ClientCert,
		ClientKey:      pubCfg.ClientKey,
	}

	// Raw TLS handshake straight to the broker, outside paho. paho collapses
	// every connect-time failure to a bare "EOF" — this reports the actual
	// reason (bad CA, hostname/SNI mismatch, network drop) if TLS is the
	// problem, before we even try the real MQTT connect.
	if err := mqttpub.PreflightTLS(emqxOpts); err != nil {
		logger.Fatalf("EMQX TLS preflight failed: %v", err)
	}

	pubOpts, err := mqttpub.BuildEMQXClientOptions(emqxOpts)
	if err != nil {
		logger.Fatalf("Failed to build EMQX publisher options: %v", err)
	}

	// Connect to the local edge Mosquitto once, up front, and share this
	// single connection for both reply-listening and the PLC-data
	// subscription below (mqtts.Run) — no need for a second connection to
	// the same broker.
	mosquittoClient, err := mqtts.Connect(config.GetMqttConfig())
	if err != nil {
		logger.Fatalf("Failed to connect to Mosquitto: %v", err)
	}

	pub, err := mqttpub.NewPublisher(pubOpts, mosquittoClient, pubCfg.InsertRequestTopic, pubCfg.ReplyTopicPrefix)
	if err != nil {
		logger.Fatalf("Failed to connect EMQX publisher: %v", err)
	}
	patch.Pub = pub
	defer pub.Close()

	// --- Heartbeat — periodic liveness ping over the same publish path ---
	hbCfg := config.GetHeartbeatConfig()
	stopHeartbeat := make(chan struct{})
	pub.StartHeartbeat(stopHeartbeat, hbCfg.TenantID, hbCfg.DeviceID, hbCfg.Version, startTime, hbCfg.Interval)

	// Channels for communication and termination
	stopProcessing := make(chan struct{})
	clientDone := make(chan struct{})

	// Channel for receiving MQTT messages as JSON strings
	receivedMessagesJSONChan := make(chan string, 1000)

	// Run the PLC-data subscription on the SAME connection used for reply
	// listening above (mosquittoClient) — subscribes cfg.Topic and blocks
	// until shutdown.
	go mqtts.Run(
		mosquittoClient,
		config.GetMqttConfig(),
		receivedMessagesJSONChan,
		clientDone,
	)

	// Process MQTT data
	go func() {
		defer close(clientDone)
		for {
			select {
			case <-stopProcessing:
				return
			default:
				handler.ProcessMQTTData(
					config.GetAppConfig(), receivedMessagesJSONChan, plcApp)
			}
		}
	}()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	// Initiate graceful shutdown
	close(stopHeartbeat)
	close(stopProcessing)
	// Wait for client to finish
	<-clientDone
}
