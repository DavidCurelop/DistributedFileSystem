package security

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestUserTokenCredentials(t *testing.T) {
	creds := NewUserTokenCredentials("my-secret", "alice")
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if md[authorizationHeader] != "Bearer my-secret" {
		t.Fatalf("expected Bearer my-secret, got %q", md[authorizationHeader])
	}
	if md[UserHeader] != "alice" {
		t.Fatalf("expected alice, got %q", md[UserHeader])
	}

	defaultCreds := NewTokenCredentials("my-secret")
	defaultMd, err := defaultCreds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if defaultMd[UserHeader] != DefaultUser {
		t.Fatalf("expected default user, got %q", defaultMd[UserHeader])
	}
}

func TestUserFromContext(t *testing.T) {
	ctx := context.Background()
	if user := UserFromContext(ctx); user != DefaultUser {
		t.Fatalf("expected DefaultUser, got %q", user)
	}

	inboundMD := metadata.New(map[string]string{
		UserHeader: "bob",
	})
	ctxWithUser := metadata.NewIncomingContext(ctx, inboundMD)
	if user := UserFromContext(ctxWithUser); user != "bob" {
		t.Fatalf("expected bob, got %q", user)
	}

	emptyUserMD := metadata.New(map[string]string{
		UserHeader: "   ",
	})
	ctxWithEmptyUser := metadata.NewIncomingContext(ctx, emptyUserMD)
	if user := UserFromContext(ctxWithEmptyUser); user != DefaultUser {
		t.Fatalf("expected DefaultUser, got %q", user)
	}
}
