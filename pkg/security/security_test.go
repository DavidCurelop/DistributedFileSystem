package security

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testToken = "test-token"

func startServer(t *testing.T, certFile, keyFile string) string {
	t.Helper()
	options, err := ServerOptions(certFile, keyFile, testToken)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(options...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func check(t *testing.T, address string, ctx context.Context, opts ...grpc.DialOption) error {
	t.Helper()
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(callCtx, &healthpb.HealthCheckRequest{})
	return err
}

func TestAuthAndTransport(t *testing.T) {
	address := startServer(t, "", "")

	good, _ := ClientDialOptions("", testToken)
	if err := check(t, address, context.Background(), good...); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}

	bad, _ := ClientDialOptions("", "wrong")
	if code := status.Code(check(t, address, context.Background(), bad...)); code != codes.Unauthenticated {
		t.Fatalf("wrong token: got %v, want Unauthenticated", code)
	}

	// TLS but no token at all.
	transport, _ := ClientCredentials("")
	noToken := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	err := check(t, address, context.Background(), noToken...)
	if status.Code(err) != codes.Unauthenticated || status.Convert(err).Message() != "unauthenticated: invalid or missing token" {
		t.Fatalf("missing token: got %v", err)
	}

	// The x-auth-token header is accepted as well.
	xCtx := metadata.AppendToOutgoingContext(context.Background(), "x-auth-token", testToken)
	if err := check(t, address, xCtx, noToken...); err != nil {
		t.Fatalf("x-auth-token rejected: %v", err)
	}

	// Plain-text clients cannot talk to a TLS server.
	if err := check(t, address, context.Background(), grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
		t.Fatal("plain-text connection was accepted")
	}
}

func TestPerRPCCredentialsRefuseInsecureTransport(t *testing.T) {
	conn, err := grpc.NewClient("127.0.0.1:1",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(NewTokenCredentials(testToken)),
	)
	if err == nil {
		conn.Close()
		t.Fatal("token credentials were accepted on a plain-text connection")
	}
}

func TestServerCertificateVerification(t *testing.T) {
	cert, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	address := startServer(t, certFile, keyFile)

	trusted, err := ClientDialOptions(certFile, testToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := check(t, address, context.Background(), trusted...); err != nil {
		t.Fatalf("verified connection failed: %v", err)
	}

	// A CA that did not sign the server certificate must be refused.
	other, _ := GenerateSelfSignedCert()
	otherFile := filepath.Join(dir, "other.pem")
	os.WriteFile(otherFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: other.Certificate[0]}), 0o600)
	untrusted, _ := ClientDialOptions(otherFile, testToken)
	if err := check(t, address, context.Background(), untrusted...); err == nil {
		t.Fatal("connection to an untrusted server certificate succeeded")
	}
}

func TestServerCredentialsRequireCertAndKeyTogether(t *testing.T) {
	if _, err := ServerCredentials("cert.pem", ""); err == nil {
		t.Fatal("expected error when only the certificate is given")
	}
}
