package hpke

import (
	"crypto"
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/cloudflare/circl/kem"
	"golang.org/x/crypto/hkdf"
)

type dhKEM interface {
	sizeDH() int
	calcDH(dh []byte, sk kem.PrivateKey, pk kem.PublicKey) error
	SeedSize() int
	DeriveKeyPair(seed []byte) (kem.PublicKey, kem.PrivateKey)
	UnmarshalBinaryPrivateKey(data []byte) (kem.PrivateKey, error)
	UnmarshalBinaryPublicKey(data []byte) (kem.PublicKey, error)
}

type kemBase struct {
	id   KEM
	name string
	crypto.Hash
}

type dhKemBase struct {
	kemBase
	dhKEM
}

func (k kemBase) Name() string       { return k.name }
func (k kemBase) SharedKeySize() int { return k.Hash.Size() }

func (k kemBase) getSuiteID() (sid [5]byte) {
	sid[0], sid[1], sid[2] = 'K', 'E', 'M'
	binary.BigEndian.PutUint16(sid[3:5], uint16(k.id))
	return
}

func (k kemBase) extractExpand(dh, kemCtx []byte) []byte {
	eaePkr := k.labeledExtract([]byte(""), []byte("eae_prk"), dh)
	return k.labeledExpand(
		eaePkr,
		[]byte("shared_secret"),
		kemCtx,
		uint16(k.Size()),
	)
}

func (k kemBase) labeledExtract(salt, label, info []byte) []byte {
	suiteID := k.getSuiteID()
	labeledIKM := append(append(append(append(
		make([]byte, 0, len(versionLabel)+len(suiteID)+len(label)+len(info)),
		versionLabel...),
		suiteID[:]...),
		label...),
		info...)
	return hkdf.Extract(k.New, labeledIKM, salt)
}

func (k kemBase) labeledExpand(prk, label, info []byte, l uint16) []byte {
	suiteID := k.getSuiteID()
	labeledInfo := make(
		[]byte,
		2,
		2+len(versionLabel)+len(suiteID)+len(label)+len(info),
	)
	binary.BigEndian.PutUint16(labeledInfo[0:2], l)
	labeledInfo = append(append(append(append(labeledInfo,
		versionLabel...),
		suiteID[:]...),
		label...),
		info...)
	b := make([]byte, l)
	rd := hkdf.Expand(k.New, prk, labeledInfo)
	if _, err := io.ReadFull(rd, b); err != nil {
		panic(err)
	}
	return b
}

func (k dhKemBase) AuthEncapsulate(pkr kem.PublicKey, sks kem.PrivateKey) (
	ct []byte, ss []byte, err error,
) {
	seed := make([]byte, k.SeedSize())
	_, err = io.ReadFull(rand.Reader, seed)
	if err != nil {
		return nil, nil, err
	}

	return k.authEncap(pkr, sks, seed)
}

func (k dhKemBase) Encapsulate(pkr kem.PublicKey) (
	ct []byte, ss []byte, err error,
) {
	seed := make([]byte, k.SeedSize())
	_, err = io.ReadFull(rand.Reader, seed)
	if err != nil {
		return nil, nil, err
	}

	return k.encap(pkr, seed)
}

func (k dhKemBase) AuthEncapsulateDeterministically(
	pkr kem.PublicKey, sks kem.PrivateKey, seed []byte,
) (ct, ss []byte, err error) {
	return k.authEncap(pkr, sks, seed)
}

func (k dhKemBase) EncapsulateDeterministically(
	pkr kem.PublicKey, seed []byte,
) (ct, ss []byte, err error) {
	return k.encap(pkr, seed)
}

func (k dhKemBase) encap(
	pkR kem.PublicKey,
	seed []byte,
) (ct []byte, ss []byte, err error) {
	dh := make([]byte, k.sizeDH())
	enc, kemCtx, err := k.coreEncap(dh, pkR, seed)
	if err != nil {
		return nil, nil, err
	}
	ss = k.extractExpand(dh, kemCtx)
	return enc, ss, nil
}

func (k dhKemBase) authEncap(
	pkR kem.PublicKey,
	skS kem.PrivateKey,
	seed []byte,
) (ct []byte, ss []byte, err error) {
	dhLen := k.sizeDH()
	dh := make([]byte, 2*dhLen)
	enc, kemCtx, err := k.coreEncap(dh[:dhLen], pkR, seed)
	if err != nil {
		return nil, nil, err
	}

	err = k.calcDH(dh[dhLen:], skS, pkR)
	if err != nil {
		return nil, nil, err
	}

	pkS := skS.Public()
	pkSm, err := pkS.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	kemCtx = append(kemCtx, pkSm...)

	ss = k.extractExpand(dh, kemCtx)
	return enc, ss, nil
}

func (k dhKemBase) coreEncap(
	dh []byte,
	pkR kem.PublicKey,
	seed []byte,
) (enc []byte, kemCtx []byte, err error) {
	pkE, skE := k.DeriveKeyPair(seed)
	err = k.calcDH(dh, skE, pkR)
	if err != nil {
		return nil, nil, err
	}

	enc, err = pkE.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	pkRm, err := pkR.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	kemCtx = append(append([]byte{}, enc...), pkRm...)

	return enc, kemCtx, nil
}

func (k dhKemBase) Decapsulate(skr kem.PrivateKey, ct []byte) ([]byte, error) {
	dh := make([]byte, k.sizeDH())
	kemCtx, err := k.coreDecap(dh, skr, ct)
	if err != nil {
		return nil, err
	}
	return k.extractExpand(dh, kemCtx), nil
}

func (k dhKemBase) AuthDecapsulate(
	skR kem.PrivateKey,
	ct []byte,
	pkS kem.PublicKey,
) ([]byte, error) {
	dhLen := k.sizeDH()
	dh := make([]byte, 2*dhLen)
	kemCtx, err := k.coreDecap(dh[:dhLen], skR, ct)
	if err != nil {
		return nil, err
	}

	err = k.calcDH(dh[dhLen:], skR, pkS)
	if err != nil {
		return nil, err
	}

	pkSm, err := pkS.MarshalBinary()
	if err != nil {
		return nil, err
	}
	kemCtx = append(kemCtx, pkSm...)
	return k.extractExpand(dh, kemCtx), nil
}

func (k dhKemBase) coreDecap(
	dh []byte,
	skR kem.PrivateKey,
	ct []byte,
) ([]byte, error) {
	pkE, err := k.UnmarshalBinaryPublicKey(ct)
	if err != nil {
		return nil, err
	}

	err = k.calcDH(dh, skR, pkE)
	if err != nil {
		return nil, err
	}

	pkR := skR.Public()
	pkRm, err := pkR.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return append(append([]byte{}, ct...), pkRm...), nil
}
