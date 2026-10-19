package security

import (
	"context"
	"crypto/subtle"
	"errors"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DefaultAuthToken is used when no token is configured. It is public, so any
// real deployment should override it with -auth-token or AUTH_TOKEN.
const DefaultAuthToken = "dfsha-secret-token"

const (
	authorizationHeader = "authorization"
	tokenHeader         = "x-auth-token"
	bearerPrefix        = "bearer "
)

var errUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated: invalid or missing token")

// ValidateToken rejects an empty token and warns when the public default is in use.
func ValidateToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return errors.New("auth token must not be empty")
	}
	if token == DefaultAuthToken {
		log.Printf("WARNING: using the default auth token; set -auth-token or AUTH_TOKEN in production")
	}
	return nil
}

// authenticate checks the incoming metadata for a valid token, accepted either
// as "authorization: Bearer <token>" (or the bare token) or as "x-auth-token".
func authenticate(ctx context.Context, expected string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errUnauthenticated
	}

	var candidates []string
	for _, value := range md.Get(authorizationHeader) {
		if len(value) >= len(bearerPrefix) && strings.EqualFold(value[:len(bearerPrefix)], bearerPrefix) {
			value = value[len(bearerPrefix):]
		}
		candidates = append(candidates, value)
	}
	candidates = append(candidates, md.Get(tokenHeader)...)

	for _, candidate := range candidates {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(expected)) == 1 {
			return nil
		}
	}
	return errUnauthenticated
}

// UnaryServerInterceptor rejects unary calls that do not carry the expected token.
func UnaryServerInterceptor(expectedToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, expectedToken); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor rejects streaming calls that do not carry the expected token.
func StreamServerInterceptor(expectedToken string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), expectedToken); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

// TokenCredentials attaches the auth token to every outgoing RPC. It
// implements credentials.PerRPCCredentials and refuses to be sent over a
// connection without transport security.
type TokenCredentials struct {
	token string
}

var _ credentials.PerRPCCredentials = TokenCredentials{}

// NewTokenCredentials returns per-RPC credentials carrying token.
func NewTokenCredentials(token string) TokenCredentials {
	return TokenCredentials{token: token}
}

func (c TokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{authorizationHeader: "Bearer " + c.token}, nil
}

func (c TokenCredentials) RequireTransportSecurity() bool { return true }

// ClientDialOptions returns the dial options every outgoing connection must use:
// TLS transport credentials plus automatic token attachment.
func ClientDialOptions(caFile, token string) ([]grpc.DialOption, error) {
	transport, err := ClientCredentials(caFile)
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(NewTokenCredentials(token)),
	}, nil
}

// ServerOptions returns the options every gRPC server must use: TLS
// credentials plus token-checking interceptors.
func ServerOptions(certFile, keyFile, token string, extraHosts ...string) ([]grpc.ServerOption, error) {
	transport, err := ServerCredentials(certFile, keyFile, extraHosts...)
	if err != nil {
		return nil, err
	}
	return []grpc.ServerOption{
		grpc.Creds(transport),
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor(token)),
		grpc.ChainStreamInterceptor(StreamServerInterceptor(token)),
	}, nil
}