package hpke

import (
	"crypto/ecdh"
	"crypto/rand"
	"fmt"

	"github.com/cloudflare/circl/kem"
)

type shortKEM struct {
	dhKemBase
	ecdh.Curve
}

func (s shortKEM) PrivateKeySize() int        { return s.byteSize() }
func (s shortKEM) SeedSize() int              { return s.byteSize() }
func (s shortKEM) CiphertextSize() int        { return 1 + 2*s.byteSize() }
func (s shortKEM) PublicKeySize() int         { return 1 + 2*s.byteSize() }
func (s shortKEM) EncapsulationSeedSize() int { return s.byteSize() }

func (s shortKEM) byteSize() int {
	var bits int
	switch s.Curve {
	case ecdh.P256():
		bits = 256
	case ecdh.P384():
		bits = 384
	case ecdh.P521():
		bits = 521
	default:
		panic(ErrInvalidKEM)
	}

	return (bits + 7) / 8
}

func (s shortKEM) sizeDH() int { return s.byteSize() }
func (s shortKEM) calcDH(dh []byte, sk kem.PrivateKey, pk kem.PublicKey) error {
	PK, ok := pk.(*shortKEMPubKey)
	if !ok {
		return ErrInvalidKEMPublicKey
	}

	SK, ok := sk.(*shortKEMPrivKey)
	if !ok {
		return ErrInvalidKEMPrivateKey
	}

	x, err := SK.priv.ECDH(&PK.pub)
	if err != nil {
		return err
	}

	copy(dh, x)
	return nil
}

// Deterministically derives a keypair from a seed. If you're unsure,
// you're better off using GenerateKey().
//
// Panics if seed is not of length SeedSize().
func (s shortKEM) DeriveKeyPair(seed []byte) (kem.PublicKey, kem.PrivateKey) {
	// Implementation based on
	// https://www.ietf.org/archive/id/draft-irtf-cfrg-hpke-07.html#name-derivekeypair
	if len(seed) != s.SeedSize() {
		panic(kem.ErrSeedSize)
	}

	bitmask := byte(0xFF)
	if s.Curve == ecdh.P521() {
		bitmask = 0x01
	}

	dkpPrk := s.labeledExtract([]byte(""), []byte("dkp_prk"), seed)
	for ctr := 0; ctr <= 255; ctr++ {
		bytes := s.labeledExpand(
			dkpPrk,
			[]byte("candidate"),
			[]byte{byte(ctr)},
			uint16(s.byteSize()),
		)
		bytes[0] &= bitmask
		sk, err := s.UnmarshalBinaryPrivateKey(bytes)
		if err == nil {
			return sk.Public(), sk
		}
	}

	panic(ErrInvalidKEMDeriveKey)
}

func (s shortKEM) GenerateKeyPair() (kem.PublicKey, kem.PrivateKey, error) {
	key, err := s.Curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	sk := &shortKEMPrivKey{s, key}
	return sk.Public(), sk, err
}

func (s shortKEM) UnmarshalBinaryPrivateKey(data []byte) (kem.PrivateKey, error) {
	key, err := s.Curve.NewPrivateKey(data)
	if err != nil {
		return nil, err
	}

	return &shortKEMPrivKey{s, key}, nil
}

func (s shortKEM) UnmarshalBinaryPublicKey(data []byte) (kem.PublicKey, error) {
	key, err := s.Curve.NewPublicKey(data)
	if err != nil {
		return nil, err
	}

	return &shortKEMPubKey{s, *key}, nil
}

type shortKEMPubKey struct {
	scheme shortKEM
	pub    ecdh.PublicKey
}

func (k *shortKEMPubKey) String() string                 { return fmt.Sprintf("%x", k.pub.Bytes()) }
func (k *shortKEMPubKey) Scheme() kem.Scheme             { return k.scheme }
func (k *shortKEMPubKey) MarshalBinary() ([]byte, error) { return k.pub.Bytes(), nil }

func (k *shortKEMPubKey) Equal(pk kem.PublicKey) bool {
	k1, ok := pk.(*shortKEMPubKey)
	return ok && k.scheme == k1.scheme && k.pub.Equal(&k1.pub)
}

type shortKEMPrivKey struct {
	scheme shortKEM
	priv   *ecdh.PrivateKey
}

func (k *shortKEMPrivKey) String() string                 { return fmt.Sprintf("%x", k.priv.Bytes()) }
func (k *shortKEMPrivKey) Scheme() kem.Scheme             { return k.scheme }
func (k *shortKEMPrivKey) MarshalBinary() ([]byte, error) { return k.priv.Bytes(), nil }

func (k *shortKEMPrivKey) Equal(pk kem.PrivateKey) bool {
	k1, ok := pk.(*shortKEMPrivKey)
	return ok && k.scheme == k1.scheme && k.priv.Equal(k1.priv)
}

func (k *shortKEMPrivKey) Public() kem.PublicKey {
	return &shortKEMPubKey{k.scheme, *k.priv.PublicKey()}
}
