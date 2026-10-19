package security

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptChunk(t *testing.T) {
	passphrase := "test-secret-key-12345"
	originalData := []byte("Hello, this is a secret distributed chunk payload to be encrypted at rest!")

	ciphertext, err := EncryptChunk(originalData, passphrase)
	if err != nil {
		t.Fatalf("EncryptChunk failed: %v", err)
	}

	if bytes.Equal(ciphertext, originalData) {
		t.Fatal("ciphertext should not match plaintext")
	}

	decrypted, err := DecryptChunk(ciphertext, passphrase)
	if err != nil {
		t.Fatalf("DecryptChunk failed: %v", err)
	}

	if !bytes.Equal(decrypted, originalData) {
		t.Fatalf("decrypted data %q does not match original %q", decrypted, originalData)
	}

	_, err = DecryptChunk(ciphertext, "wrong-passphrase")
	if err == nil {
		t.Fatal("expected decryption error with wrong passphrase, got nil")
	}

	if len(ciphertext) > 15 {
		corrupted := make([]byte, len(ciphertext))
		copy(corrupted, ciphertext)
		corrupted[14] ^= 0xFF
		_, err = DecryptChunk(corrupted, passphrase)
		if err == nil {
			t.Fatal("expected decryption error with corrupted ciphertext, got nil")
		}
	}
}
