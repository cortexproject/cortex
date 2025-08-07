package hpke

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"io"

	"github.com/cloudflare/circl/dh/x25519"
	"github.com/cloudflare/circl/dh/x448"
	"github.com/cloudflare/circl/kem"
)

type xKEM struct {
	dhKemBase
	size int
}

func (x xKEM) PrivateKeySize() int        { return x.size }
func (x xKEM) SeedSize() int              { return x.size }
func (x xKEM) CiphertextSize() int        { return x.size }
func (x xKEM) PublicKeySize() int         { return x.size }
func (x xKEM) EncapsulationSeedSize() int { return x.size }

func (x xKEM) sizeDH() int { return x.size }
func (x xKEM) calcDH(dh []byte, sk kem.PrivateKey, pk kem.PublicKey) error {
	PK := pk.(*xKEMPubKey)
	SK := sk.(*xKEMPrivKey)
	switch x.size {
	case x25519.Size:
		var ss, sKey, pKey x25519.Key
		copy(sKey[:], SK.priv)
		copy(pKey[:], PK.pub)
		if !x25519.Shared(&ss, &sKey, &pKey) {
			return ErrInvalidKEMSharedSecret
		}
		copy(dh, ss[:])
	case x448.Size:
		var ss, sKey, pKey x448.Key
		copy(sKey[:], SK.priv)
		copy(pKey[:], PK.pub)
		if !x448.Shared(&ss, &sKey, &pKey) {
			return ErrInvalidKEMSharedSecret
		}
		copy(dh, ss[:])
	}
	return nil
}

// Deterministically derives a keypair from a seed. If you're unsure,
// you're better off using GenerateKey().
//
// Panics if seed is not of length SeedSize().
func (x xKEM) DeriveKeyPair(seed []byte) (kem.PublicKey, kem.PrivateKey) {
	// Implementation based on
	// https://www.ietf.org/archive/id/draft-irtf-cfrg-hpke-07.html#name-derivekeypair
	if len(seed) != x.SeedSize() {
		panic(kem.ErrSeedSize)
	}
	sk := &xKEMPrivKey{scheme: x, priv: make([]byte, x.size)}
	dkpPrk := x.labeledExtract([]byte(""), []byte("dkp_prk"), seed)
	bytes := x.labeledExpand(
		dkpPrk,
		[]byte("sk"),
		nil,
		uint16(x.PrivateKeySize()),
	)
	copy(sk.priv, bytes)
	return sk.Public(), sk
}

func (x xKEM) GenerateKeyPair() (kem.PublicKey, kem.PrivateKey, error) {
	sk := &xKEMPrivKey{scheme: x, priv: make([]byte, x.PrivateKeySize())}
	_, err := io.ReadFull(rand.Reader, sk.priv)
	if err != nil {
		return nil, nil, err
	}
	return sk.Public(), sk, nil
}

func (x xKEM) UnmarshalBinaryPrivateKey(data []byte) (kem.PrivateKey, error) {
	l := x.PrivateKeySize()
	if len(data) < l {
		return nil, ErrInvalidKEMPrivateKey
	}
	sk := &xKEMPrivKey{x, make([]byte, l), nil}
	copy(sk.priv, data[:l])
	if !sk.validate() {
		return nil, ErrInvalidKEMPrivateKey
	}
	return sk, nil
}

func (x xKEM) UnmarshalBinaryPublicKey(data []byte) (kem.PublicKey, error) {
	l := x.PublicKeySize()
	if len(data) < l {
		return nil, ErrInvalidKEMPublicKey
	}
	pk := &xKEMPubKey{x, make([]byte, l)}
	copy(pk.pub, data[:l])
	if !pk.validate() {
		return nil, ErrInvalidKEMPublicKey
	}
	return pk, nil
}

type xKEMPubKey struct {
	scheme xKEM
	pub    []byte
}

func (k *xKEMPubKey) String() string     { return fmt.Sprintf("%x", k.pub) }
func (k *xKEMPubKey) Scheme() kem.Scheme { return k.scheme }
func (k *xKEMPubKey) MarshalBinary() ([]byte, error) {
	return append(make([]byte, 0, k.scheme.PublicKeySize()), k.pub...), nil
}

func (k *xKEMPubKey) Equal(pk kem.PublicKey) bool {
	k1, ok := pk.(*xKEMPubKey)
	return ok &&
		k.scheme.id == k1.scheme.id &&
		bytes.Equal(k.pub, k1.pub)
}
func (k *xKEMPubKey) validate() bool { return len(k.pub) == k.scheme.PublicKeySize() }

type xKEMPrivKey struct {
	scheme xKEM
	priv   []byte
	pub    *xKEMPubKey
}

func (k *xKEMPrivKey) String() string     { return fmt.Sprintf("%x", k.priv) }
func (k *xKEMPrivKey) Scheme() kem.Scheme { return k.scheme }
func (k *xKEMPrivKey) MarshalBinary() ([]byte, error) {
	return append(make([]byte, 0, k.scheme.PrivateKeySize()), k.priv...), nil
}

func (k *xKEMPrivKey) Equal(pk kem.PrivateKey) bool {
	k1, ok := pk.(*xKEMPrivKey)
	return ok &&
		k.scheme.id == k1.scheme.id &&
		subtle.ConstantTimeCompare(k.priv, k1.priv) == 1
}

func (k *xKEMPrivKey) Public() kem.PublicKey {
	if k.pub == nil {
		k.pub = &xKEMPubKey{scheme: k.scheme, pub: make([]byte, k.scheme.size)}
		switch k.scheme.size {
		case x25519.Size:
			var sk, pk x25519.Key
			copy(sk[:], k.priv)
			x25519.KeyGen(&pk, &sk)
			copy(k.pub.pub, pk[:])
		case x448.Size:
			var sk, pk x448.Key
			copy(sk[:], k.priv)
			x448.KeyGen(&pk, &sk)
			copy(k.pub.pub, pk[:])
		}
	}
	return k.pub
}
func (k *xKEMPrivKey) validate() bool { return len(k.priv) == k.scheme.PrivateKeySize() }
