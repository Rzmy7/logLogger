package metadata

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	DefaultKeyPrefix = "ll_live"
	KeyEntropyBytes  = 32
)

// GenerateAPIKey generates a cryptographically secure random API key and its SHA-256 hash.
// Format: {prefix}_{64-hex-characters}
func GenerateAPIKey(prefix string) (rawKey string, keyHash string, err error) {
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	prefix = strings.TrimSuffix(prefix, "_")

	bytes := make([]byte, KeyEntropyBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	randomPart := hex.EncodeToString(bytes)
	rawKey = fmt.Sprintf("%s_%s", prefix, randomPart)
	keyHash = HashKey(rawKey)

	return rawKey, keyHash, nil
}

// HashKey computes the SHA-256 hex digest of a raw API key.
func HashKey(rawKey string) string {
	rawKey = strings.TrimSpace(rawKey)
	hash := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(hash[:])
}

// VerifyKey checks if rawKey matches expectedHash in constant time.
func VerifyKey(rawKey, expectedHash string) bool {
	computed := HashKey(rawKey)
	return subtle.ConstantTimeCompare([]byte(computed), []byte(expectedHash)) == 1
}
