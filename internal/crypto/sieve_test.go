package crypto

import (
	"crypto/rand"
	"testing"
)

func TestSieveValidation(t *testing.T) {
	secret := []byte("meritual-adversarial-secret-key-1337")
	sieve := NewSieve(secret)

	payload := []byte(`{"ref":"refs/heads/main","after":"a1b2c3d4e5f6","commits":[]}`)
	validSig := Sign(secret, payload)

	// Test 1: Valid signature should pass
	if !sieve.Verify([]byte(validSig), payload) {
		t.Fatalf("expected valid signature to pass verification")
	}

	// Test 2: Forged signature should fail
	forgedSig := "sha256=0000000000000000000000000000000000000000000000000000000000000000"
	if sieve.Verify([]byte(forgedSig), payload) {
		t.Fatalf("expected forged signature to be dropped")
	}

	// Test 3: Modified payload with valid signature should fail
	tamperedPayload := []byte(`{"ref":"refs/heads/main","after":"malicious-commit"}`)
	if sieve.Verify([]byte(validSig), tamperedPayload) {
		t.Fatalf("expected tampered payload to be rejected")
	}

	// Test 4: Malformed signature header should fail gracefully
	badHeaders := []string{
		"",
		"sha256=",
		"sha256=invalid-hex",
		"md5=00000000000000000000000000000000",
		"sha256=1234567890abcdef", // too short
	}
	for _, bh := range badHeaders {
		if sieve.Verify([]byte(bh), payload) {
			t.Fatalf("expected bad header %q to fail", bh)
		}
	}
}

func BenchmarkSieveVerify(b *testing.B) {
	secret := []byte("meritual-adversarial-secret-key-1337")
	sieve := NewSieve(secret)

	payload := make([]byte, 2048) // typical webhook payload size: 2KB
	_, _ = rand.Read(payload)
	validSig := []byte(Sign(secret, payload))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !sieve.Verify(validSig, payload) {
			b.Fatalf("failed verification")
		}
	}
}
