package mqttpub

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// EMQXOptions holds everything needed to connect to EMQX (the request-
// publishing side). This is deliberately separate from Mosquitto config —
// pub and reply-listen may be different brokers entirely.
type EMQXOptions struct {
	Broker   string // host only, no scheme
	Port     string
	Username string
	Password string

	// ClientIDPrefix is prepended to a fresh UUID for each connection
	// (e.g. "gopub-edge-mcs8_publisher_"). Defaults to "gopub-edge_publisher_"
	// if left empty. Worth setting per-deployment so you can tell clients
	// apart in the EMQX dashboard when running multiple edge units.
	ClientIDPrefix string

	// TLS is optional. Leave UseTLS false to connect plain tcp:// (fine for
	// a local/LAN EMQX). Set UseTLS true for ssl://. CACert alone gives
	// you server-verified TLS; also set ClientCert/ClientKey for mTLS.
	UseTLS     bool
	CACert     string // PEM, optional even when UseTLS is true
	ClientCert string // PEM, optional (mTLS)
	ClientKey  string // PEM, optional (mTLS)
}

// buildTLSConfigFromCerts is shared by EMQXOptions and MosquittoOptions so
// both use identical CA/cert-loading logic — no risk of a preflight check
// passing against a different trust config than the real connection uses.
func buildTLSConfigFromCerts(caCert, clientCert, clientKey string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	if caCert != "" {
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM([]byte(caCert)) {
			return nil, fmt.Errorf("mqttpub: failed to append CA certificate")
		}
		tlsConfig.RootCAs = caPool
	}

	if clientCert != "" && clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return nil, fmt.Errorf("mqttpub: error loading client certificate/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// BuildEMQXClientOptions builds mqtt.ClientOptions for the connection to
// EMQX, using real username/password auth.
func BuildEMQXClientOptions(o EMQXOptions) (*mqtt.ClientOptions, error) {
	opts := mqtt.NewClientOptions()

	// NOTE: "ssl" here, not "mqtts". paho.mqtt.golang's scheme parser
	// reliably recognizes tcp/ssl/tls/ws/wss — "mqtts" is not guaranteed
	// to be handled the same way across versions.
	scheme := "tcp"
	if o.UseTLS {
		scheme = "ssl"
	}
	opts.AddBroker(fmt.Sprintf("%s://%s:%s", scheme, o.Broker, o.Port))
	prefix := o.ClientIDPrefix
	if prefix == "" {
		prefix = "gopub-edge_publisher_"
	}
	opts.SetClientID(prefix + uuid.New().String())
	opts.SetUsername(o.Username)
	opts.SetPassword(o.Password)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(30 * time.Second)

	if o.UseTLS {
		tlsConfig, err := buildTLSConfigFromCerts(o.CACert, o.ClientCert, o.ClientKey)
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsConfig)
	}

	return opts, nil
}

// PreflightTLS does a raw TLS handshake straight to the broker, completely
// outside paho, so a bad cert/network path fails with a clear reason
// (bad CA, expired cert, hostname/SNI mismatch, TCP-level drop, timeout)
// instead of paho's generic "EOF". No-op when UseTLS is false.
func PreflightTLS(o EMQXOptions) error {
	if !o.UseTLS {
		return nil
	}

	tlsConfig, err := buildTLSConfigFromCerts(o.CACert, o.ClientCert, o.ClientKey)
	if err != nil {
		return err
	}
	tlsConfig.ServerName = o.Broker

	addr := net.JoinHostPort(o.Broker, o.Port)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("mqttpub: TLS preflight to %s failed: %w", addr, err)
	}
	defer conn.Close()

	state := conn.ConnectionState()
	if len(state.PeerCertificates) > 0 {
		cert := state.PeerCertificates[0]
		log.Printf("[mqttpub] ✓ TLS preflight OK — %s, server cert CN=%q issuer=%q expires=%s",
			tlsVersionName(state.Version), cert.Subject.CommonName, cert.Issuer.CommonName, cert.NotAfter.Format(time.RFC3339))
	} else {
		log.Printf("[mqttpub] ✓ TLS preflight OK — %s (server presented no certificates)", tlsVersionName(state.Version))
	}
	return nil
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	case tls.VersionTLS13:
		return "TLS1.3"
	default:
		return fmt.Sprintf("TLS(0x%x)", v)
	}
}

// EnableDebugLogging wires paho's internal logger to stdout. Verbose —
// logs every packet paho sends/receives — so gate it behind an env var
// (e.g. MQTT_DEBUG=true) rather than leaving it on in production.
func EnableDebugLogging() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	mqtt.DEBUG = logger
	mqtt.WARN = logger
	mqtt.ERROR = logger
	mqtt.CRITICAL = logger
}
