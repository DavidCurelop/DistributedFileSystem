// Package security provides the transport encryption (TLS) and token
// authentication shared by the Client, ControlNode and DataNode binaries.
package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"google.golang.org/grpc/credentials"
)

const selfSignedValidity = 365 * 24 * time.Hour

// GenerateSelfSignedCert creates an in-memory, self-signed ECDSA P-256
// certificate valid for localhost, the loopback addresses, the machine
// hostname and any extra hosts given (DNS names or IP addresses).
func GenerateSelfSignedCert(extraHosts ...string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating TLS key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating certificate serial number: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{"DFSha"}, CommonName: "dfsha"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(selfSignedValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	hosts := extraHosts
	if hostname, err := os.Hostname(); err == nil {
		hosts = append(hosts, hostname)
	}
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("creating self-signed certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// ServerCredentials returns the TLS credentials for a gRPC server. With both
// certFile and keyFile set it loads them from disk; with neither it generates
// a self-signed certificate in memory so the server is encrypted out of the
// box. Providing only one of the two is an error.
func ServerCredentials(certFile, keyFile string, extraHosts ...string) (credentials.TransportCredentials, error) {
	var cert tls.Certificate
	var err error

	switch {
	case certFile != "" && keyFile != "":
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading TLS key pair (%q, %q): %w", certFile, keyFile, err)
		}
	case certFile == "" && keyFile == "":
		cert, err = GenerateSelfSignedCert(extraHosts...)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("tls-cert and tls-key must be provided together")
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// ClientCredentials returns the TLS credentials for a gRPC client. When
// caFile is set the server certificate is verified against that PEM bundle
// (a self-signed server certificate is its own CA). When it is empty the
// channel is still encrypted, but the server identity is NOT verified, which
// is what lets freshly generated self-signed certificates work with no setup.
func ClientCredentials(caFile string) (credentials.TransportCredentials, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}

	if caFile == "" {
		config.InsecureSkipVerify = true // #nosec G402 -- documented opt-out when no CA is configured
		return credentials.NewTLS(config), nil
	}

	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading TLS CA file %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in TLS CA file %q", caFile)
	}
	config.RootCAs = pool
	return credentials.NewTLS(config), nil
}