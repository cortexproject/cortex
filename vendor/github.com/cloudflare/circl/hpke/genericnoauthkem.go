package hpke

// Shim to use generic KEM (kem.Scheme) as HPKE KEM.

import (
	"github.com/cloudflare/circl/internal/sha3"
	"github.com/cloudflare/circl/kem"
)

// genericNoAuthKEM wraps a generic KEM (kem.Scheme) to be used as a HPKE KEM.
type genericNoAuthKEM struct {
	kem.Scheme
	name string
}

func (h genericNoAuthKEM) Name() string { return h.name }

// HPKE requires DeriveKeyPair() to take any seed larger than the private key
// size, whereas typical KEMs expect a specific seed size. We'll just use
// SHAKE256 to hash it to the right size as in X-Wing.
func (h genericNoAuthKEM) DeriveKeyPair(seed []byte) (kem.PublicKey, kem.PrivateKey) {
	seed2 := make([]byte, h.Scheme.SeedSize())
	hh := sha3.NewShake256()
	_, _ = hh.Write(seed)
	_, _ = hh.Read(seed2)
	return h.Scheme.DeriveKeyPair(seed2)
}
