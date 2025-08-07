package hpke

// This file implements a hybrid KEM for HPKE using a simple concatenation
// combiner.
//
// WARNING It is not safe to combine arbitrary KEMs using this combiner.
// See the draft specification for more details:
// https://bwesterb.github.io/draft-westerbaan-cfrg-hpke-xyber768d00/draft-westerbaan-cfrg-hpke-xyber768d00.html#name-security-considerations

import (
	"crypto/rand"

	"github.com/cloudflare/circl/kem"
)

type hybridKEM struct {
	kemBase
	kemA kem.Scheme
	kemB kem.Scheme
}

func (h hybridKEM) PrivateKeySize() int { return h.kemA.PrivateKeySize() + h.kemB.PrivateKeySize() }
func (h hybridKEM) SeedSize() int       { return 32 }
func (h hybridKEM) CiphertextSize() int { return h.kemA.CiphertextSize() + h.kemB.CiphertextSize() }
func (h hybridKEM) PublicKeySize() int  { return h.kemA.PublicKeySize() + h.kemB.PublicKeySize() }
func (h hybridKEM) EncapsulationSeedSize() int {
	return h.kemA.EncapsulationSeedSize() + h.kemB.EncapsulationSeedSize()
}
func (h hybridKEM) SharedKeySize() int { return h.kemA.SharedKeySize() + h.kemB.SharedKeySize() }
func (h hybridKEM) Name() string       { return h.name }

func (h hybridKEM) AuthDecapsulate(skR kem.PrivateKey,
	ct []byte,
	pkS kem.PublicKey,
) ([]byte, error) {
	panic("AuthDecapsulate is not supported for this KEM")
}

func (h hybridKEM) AuthEncapsulate(pkr kem.PublicKey, sks kem.PrivateKey) (
	ct []byte, ss []byte, err error,
) {
	panic("AuthEncapsulate is not supported for this KEM")
}

func (h hybridKEM) AuthEncapsulateDeterministically(pkr kem.PublicKey, sks kem.PrivateKey, seed []byte) (ct, ss []byte, err error) {
	panic("AuthEncapsulateDeterministically is not supported for this KEM")
}

func (h hybridKEM) Encapsulate(pkr kem.PublicKey) (
	ct []byte, ss []byte, err error,
) {
	panic("Encapsulate is not implemented")
}

func (h hybridKEM) Decapsulate(skr kem.PrivateKey, ct []byte) ([]byte, error) {
	hybridSk := skr.(*hybridKEMPrivKey)
	ssA, err := h.kemA.Decapsulate(hybridSk.privA, ct[0:h.kemA.CiphertextSize()])
	if err != nil {
		return nil, err
	}
	ssB, err := h.kemB.Decapsulate(hybridSk.privB, ct[h.kemA.CiphertextSize():])
	if err != nil {
		return nil, err
	}

	ss := append(ssA, ssB...)

	return ss, nil
}

func (h hybridKEM) EncapsulateDeterministically(
	pkr kem.PublicKey, seed []byte,
) (ct, ss []byte, err error) {
	hybridPk := pkr.(*hybridKEMPubKey)
	encA, ssA, err := h.kemA.EncapsulateDeterministically(hybridPk.pubA, seed[0:h.kemA.EncapsulationSeedSize()])
	if err != nil {
		return nil, nil, err
	}
	encB, ssB, err := h.kemB.EncapsulateDeterministically(hybridPk.pubB, seed[h.kemA.EncapsulationSeedSize():])
	if err != nil {
		return nil, nil, err
	}

	ct = append(encA, encB...)
	ss = append(ssA, ssB...)

	return ct, ss, nil
}

type hybridKEMPrivKey struct {
	scheme kem.Scheme
	privA  kem.PrivateKey
	privB  kem.PrivateKey
}

func (k *hybridKEMPrivKey) Scheme() kem.Scheme {
	return k.scheme
}

func (k *hybridKEMPrivKey) MarshalBinary() ([]byte, error) {
	skA, err := k.privA.MarshalBinary()
	if err != nil {
		return nil, err
	}
	skB, err := k.privB.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(skA, skB...), nil
}

func (k *hybridKEMPrivKey) Equal(sk kem.PrivateKey) bool {
	k1, ok := sk.(*hybridKEMPrivKey)
	return ok &&
		k.privA.Equal(k1.privA) &&
		k.privB.Equal(k1.privB)
}

func (k *hybridKEMPrivKey) Public() kem.PublicKey {
	return &hybridKEMPubKey{
		scheme: k.scheme,
		pubA:   k.privA.Public(),
		pubB:   k.privB.Public(),
	}
}

type hybridKEMPubKey struct {
	scheme kem.Scheme
	pubA   kem.PublicKey
	pubB   kem.PublicKey
}

func (k *hybridKEMPubKey) Scheme() kem.Scheme {
	return k.scheme
}

func (k hybridKEMPubKey) MarshalBinary() ([]byte, error) {
	pkA, err := k.pubA.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pkB, err := k.pubB.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(pkA, pkB...), nil
}

func (k *hybridKEMPubKey) Equal(pk kem.PublicKey) bool {
	k1, ok := pk.(*hybridKEMPubKey)
	return ok &&
		k.pubA.Equal(k1.pubA) &&
		k.pubB.Equal(k1.pubB)
}

// Deterministically derives a keypair from a seed. If you're unsure,
// you're better off using GenerateKey().
//
// Panics if seed is not of length SeedSize().
func (h hybridKEM) DeriveKeyPair(seed []byte) (kem.PublicKey, kem.PrivateKey) {
	// Implementation based on
	// https://www.ietf.org/archive/id/draft-irtf-cfrg-hpke-07.html#name-derivekeypair
	if len(seed) != h.SeedSize() {
		panic(kem.ErrSeedSize)
	}

	outputSeedSize := h.kemA.SeedSize() + h.kemB.SeedSize()
	dkpPrk := h.labeledExtract([]byte(""), []byte("dkp_prk"), seed)
	bytes := h.labeledExpand(
		dkpPrk,
		[]byte("sk"),
		nil,
		uint16(outputSeedSize),
	)
	seedA := bytes[0:h.kemA.SeedSize()]
	seedB := bytes[h.kemA.SeedSize():]
	pubA, privA := h.kemA.DeriveKeyPair(seedA)
	pubB, privB := h.kemB.DeriveKeyPair(seedB)

	privKey := &hybridKEMPrivKey{
		privA: privA,
		privB: privB,
	}
	pubKey := &hybridKEMPubKey{
		pubA: pubA,
		pubB: pubB,
	}

	return pubKey, privKey
}

func (h hybridKEM) GenerateKeyPair() (kem.PublicKey, kem.PrivateKey, error) {
	seed := make([]byte, h.SeedSize())
	_, err := rand.Read(seed)
	if err != nil {
		return nil, nil, err
	}
	pk, sk := h.DeriveKeyPair(seed)
	return pk, sk, nil
}

func (h hybridKEM) UnmarshalBinaryPrivateKey(data []byte) (kem.PrivateKey, error) {
	skA, err := h.kemA.UnmarshalBinaryPrivateKey(data[0:h.kemA.PrivateKeySize()])
	if err != nil {
		return nil, err
	}
	skB, err := h.kemB.UnmarshalBinaryPrivateKey(data[h.kemA.PrivateKeySize():])
	if err != nil {
		return nil, err
	}

	return &hybridKEMPrivKey{
		privA: skA,
		privB: skB,
	}, nil
}

func (h hybridKEM) UnmarshalBinaryPublicKey(data []byte) (kem.PublicKey, error) {
	pkA, err := h.kemA.UnmarshalBinaryPublicKey(data[0:h.kemA.PublicKeySize()])
	if err != nil {
		return nil, err
	}
	pkB, err := h.kemB.UnmarshalBinaryPublicKey(data[h.kemA.PublicKeySize():])
	if err != nil {
		return nil, err
	}

	return &hybridKEMPubKey{
		pubA: pkA,
		pubB: pkB,
	}, nil
}
