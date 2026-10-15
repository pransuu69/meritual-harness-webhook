package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"hash"
	"sync"
)

// Sieve implements a high-throughput, zero-allocation cryptographic verification engine
// for GitHub HMAC-SHA256 signatures (X-Hub-Signature-256).
type Sieve struct {
	secret []byte
	pool   sync.Pool
}

// NewSieve creates an initialized Sieve using a shared webhook secret.
func NewSieve(secret []byte) *Sieve {
	s := &Sieve{
		secret: secret,
	}
	s.pool.New = func() any {
		return hmac.New(sha256.New, s.secret)
	}
	return s
}

// Verify checks whether the provided header signature matches the HMAC-SHA256 of the body.
// It executes in constant time to prevent timing attacks and avoids heap allocations.
// Expected header format: "sha256=<64 hex chars>".
func (s *Sieve) Verify(headerSig []byte, body []byte) bool {
	// Fast check: signature header must start with "sha256=" and be exactly 71 bytes (7 + 64)
	const prefix = "sha256="
	const prefixLen = len(prefix)
	const hexSigLen = 64
	const totalLen = prefixLen + hexSigLen

	if len(headerSig) != totalLen {
		return false
	}

	for i := 0; i < prefixLen; i++ {
		if headerSig[i] != prefix[i] {
			return false
		}
	}

	var expectedMAC [32]byte
	if !decodeHex64(headerSig[prefixLen:], &expectedMAC) {
		return false
	}

	h := s.pool.Get().(hash.Hash)
	h.Reset()
	h.Write(body)
	var computedMAC [32]byte
	h.Sum(computedMAC[:0])
	s.pool.Put(h)

	// Constant-time comparison
	return subtle.ConstantTimeCompare(expectedMAC[:], computedMAC[:]) == 1
}

// Sign produces the formatted "sha256=<hex>" string for a given payload, useful for tests/harness.
func Sign(secret []byte, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	sum := mac.Sum(nil)
	return "sha256=" + hex.EncodeToString(sum)
}

// decodeHex64 decodes a 64-byte ASCII hex slice into a [32]byte buffer without allocations.
func decodeHex64(src []byte, dst *[32]byte) bool {
	if len(src) != 64 {
		return false
	}
	for i := 0; i < 32; i++ {
		hi := fromHexChar(src[i*2])
		lo := fromHexChar(src[i*2+1])
		if hi == 255 || lo == 255 {
			return false
		}
		dst[i] = (hi << 4) | lo
	}
	return true
}

func fromHexChar(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	default:
		return 255
	}
}
