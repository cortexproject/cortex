package tls

import (
	"errors"
	"fmt"

	"github.com/cloudflare/circl/hpke"
	"github.com/cloudflare/circl/kem"
)

type HPKERawPublicKey = []byte
type HPKE_KEM_ID = uint16  // RFC 9180
type HPKE_KDF_ID = uint16  // RFC 9180
type HPKE_AEAD_ID = uint16 // RFC 9180

type HPKESymmetricCipherSuite struct {
	KdfId  HPKE_KDF_ID
	AeadId HPKE_AEAD_ID
}

type HPKEKeyConfig struct {
	ConfigId     uint8
	KemId        HPKE_KEM_ID
	PublicKey    kem.PublicKey
	rawPublicKey HPKERawPublicKey
	CipherSuites []HPKESymmetricCipherSuite
}

var defaultHPKESuite hpke.Suite

func init() {
	var err error
	defaultHPKESuite, err = hpkeAssembleSuite(
		uint16(hpke.KEM_X25519_HKDF_SHA256),
		uint16(hpke.KDF_HKDF_SHA256),
		uint16(hpke.AEAD_AES128GCM),
	)
	if err != nil {
		panic(fmt.Sprintf("hpke: mandatory-to-implement cipher suite not supported: %s", err))
	}
}

func hpkeAssembleSuite(kemId, kdfId, aeadId uint16) (hpke.Suite, error) {
	kem := hpke.KEM(kemId)
	if !kem.IsValid() {
		return hpke.Suite{}, errors.New("KEM is not supported")
	}
	kdf := hpke.KDF(kdfId)
	if !kdf.IsValid() {
		return hpke.Suite{}, errors.New("KDF is not supported")
	}
	aead := hpke.AEAD(aeadId)
	if !aead.IsValid() {
		return hpke.Suite{}, errors.New("AEAD is not supported")
	}
	return hpke.NewSuite(kem, kdf, aead), nil
}

var dummyX25519PublicKey = []byte{
	143, 38, 37, 36, 12, 6, 229, 30, 140, 27, 167, 73, 26, 100, 203, 107, 216,
	81, 163, 222, 52, 211, 54, 210, 46, 37, 78, 216, 157, 97, 241, 244,
}
