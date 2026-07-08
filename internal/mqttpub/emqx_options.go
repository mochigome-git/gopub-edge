package mqttpub

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// EMQXOptions holds everything needed to connect the publisher to EMQX.
// This is deliberately separate from whatever config the subscriber uses
// for Mosquitto — pub and sub are different brokers now.
type EMQXOptions struct {
	Broker   string // host only, no scheme
	Port     string
	Username string
	Password string

	// TLS is optional. Leave UseTLS false to connect plain tcp:// (fine for
	// a local/LAN EMQX). Set UseTLS true for mqtts://. CACert alone gives
	// you server-verified TLS; also set ClientCert/ClientKey for mTLS.
	UseTLS     bool
	CACert     string // PEM, optional even when UseTLS is true
	ClientCert string // PEM, optional (mTLS)
	ClientKey  string // PEM, optional (mTLS)
}

// BuildEMQXClientOptions builds mqtt.ClientOptions for the publisher's
// connection to EMQX, using real username/password auth instead of the
// hardcoded "emqx"/"public" the Mosquitto subscriber client uses.
func BuildEMQXClientOptions(o EMQXOptions) (*mqtt.ClientOptions, error) {
	opts := mqtt.NewClientOptions()

	scheme := "tcp"
	if o.UseTLS {
		scheme = "mqtts"
	}
	opts.AddBroker(fmt.Sprintf("%s://%s:%s", scheme, o.Broker, o.Port))
	opts.SetClientID("gopatch_publisher_" + uuid.New().String())
	opts.SetUsername(o.Username)
	opts.SetPassword(o.Password)
	opts.SetAutoReconnect(true)

	if o.UseTLS {
		tlsConfig := &tls.Config{}

		if o.CACert != "" {
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM([]byte(o.CACert)) {
				return nil, fmt.Errorf("mqttpub: failed to append EMQX CA certificate")
			}
			tlsConfig.RootCAs = caPool
		}

		if o.ClientCert != "" && o.ClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(o.ClientCert), []byte(o.ClientKey))
			if err != nil {
				return nil, fmt.Errorf("mqttpub: error loading EMQX client certificate/key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		opts.SetTLSConfig(tlsConfig)
	}

	return opts, nil
}
