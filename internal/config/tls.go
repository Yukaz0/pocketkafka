package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

func (t TLS) Any() bool { return t.Enabled || t.HTTP || t.MQTT }

func (t TLS) MinTLSVersion() uint16 {
	if strings.TrimSpace(t.MinVersion) == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

var allowedCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

func (t TLS) ServerConfig() (*tls.Config, error) {
	if !t.Any() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}
	cfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       t.MinTLSVersion(),
		CipherSuites:     allowedCipherSuites,
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
	}
	if t.ClientCAFile != "" {
		pem, err := os.ReadFile(t.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TLS client CA %s contains no certificates", t.ClientCAFile)
		}

		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func ClientPrincipal(state *tls.ConnectionState) string {
	if state == nil || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return ""
	}
	leaf := state.PeerCertificates[0]
	if cn := strings.TrimSpace(leaf.Subject.CommonName); cn != "" {
		return cn
	}
	if len(leaf.DNSNames) > 0 {
		return strings.TrimSpace(leaf.DNSNames[0])
	}
	return ""
}
