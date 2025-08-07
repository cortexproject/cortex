package hpke

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	_ "crypto/sha256" // Linking sha256.
	_ "crypto/sha512" // Linking sha512.
	"fmt"
	"hash"
	"io"

	"github.com/cloudflare/circl/dh/x25519"
	"github.com/cloudflare/circl/dh/x448"
	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/kyber/kyber768"
	"github.com/cloudflare/circl/kem/xwing"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

type KEM uint16

//nolint:golint,stylecheck
const (
	// KEM_P256_HKDF_SHA256 is a KEM using P256 curve and HKDF with SHA-256.
	KEM_P256_HKDF_SHA256 KEM = 0x10
	// KEM_P384_HKDF_SHA384 is a KEM using P384 curve and HKDF with SHA-384.
	KEM_P384_HKDF_SHA384 KEM = 0x11
	// KEM_P521_HKDF_SHA512 is a KEM using P521 curve and HKDF with SHA-512.
	KEM_P521_HKDF_SHA512 KEM = 0x12
	// KEM_X25519_HKDF_SHA256 is a KEM using X25519 Diffie-Hellman function
	// and HKDF with SHA-256.
	KEM_X25519_HKDF_SHA256 KEM = 0x20
	// KEM_X448_HKDF_SHA512 is a KEM using X448 Diffie-Hellman function and
	// HKDF with SHA-512.
	KEM_X448_HKDF_SHA512 KEM = 0x21
	// KEM_X25519_KYBER768_DRAFT00 is a hybrid KEM built on DHKEM(X25519, HKDF-SHA256)
	// and Kyber768Draft00
	KEM_X25519_KYBER768_DRAFT00 KEM = 0x30
	// KEM_XWING is a hybrid KEM using X25519 and ML-KEM-768.
	KEM_XWING KEM = 0x647a
)

// IsValid returns true if the KEM identifier is supported by the HPKE package.
func (k KEM) IsValid() bool {
	switch k {
	case KEM_P256_HKDF_SHA256,
		KEM_P384_HKDF_SHA384,
		KEM_P521_HKDF_SHA512,
		KEM_X25519_HKDF_SHA256,
		KEM_X448_HKDF_SHA512,
		KEM_X25519_KYBER768_DRAFT00,
		KEM_XWING:
		return true
	default:
		return false
	}
}

// Scheme returns an instance of a KEM that supports authentication. Panics if
// the KEM identifier is invalid.
func (k KEM) Scheme() kem.Scheme {
	switch k {
	case KEM_P256_HKDF_SHA256:
		return dhkemp256hkdfsha256
	case KEM_P384_HKDF_SHA384:
		return dhkemp384hkdfsha384
	case KEM_P521_HKDF_SHA512:
		return dhkemp521hkdfsha512
	case KEM_X25519_HKDF_SHA256:
		return dhkemx25519hkdfsha256
	case KEM_X448_HKDF_SHA512:
		return dhkemx448hkdfsha512
	case KEM_X25519_KYBER768_DRAFT00:
		return hybridkemX25519Kyber768
	case KEM_XWING:
		return kemXwing
	default:
		panic(ErrInvalidKEM)
	}
}

type KDF uint16

//nolint:golint,stylecheck
const (
	// KDF_HKDF_SHA256 is a KDF using HKDF with SHA-256.
	KDF_HKDF_SHA256 KDF = 0x01
	// KDF_HKDF_SHA384 is a KDF using HKDF with SHA-384.
	KDF_HKDF_SHA384 KDF = 0x02
	// KDF_HKDF_SHA512 is a KDF using HKDF with SHA-512.
	KDF_HKDF_SHA512 KDF = 0x03
)

func (k KDF) IsValid() bool {
	switch k {
	case KDF_HKDF_SHA256,
		KDF_HKDF_SHA384,
		KDF_HKDF_SHA512:
		return true
	default:
		return false
	}
}

// ExtractSize returns the size (in bytes) of the pseudorandom key produced
// by KDF.Extract.
func (k KDF) ExtractSize() int {
	switch k {
	case KDF_HKDF_SHA256:
		return crypto.SHA256.Size()
	case KDF_HKDF_SHA384:
		return crypto.SHA384.Size()
	case KDF_HKDF_SHA512:
		return crypto.SHA512.Size()
	default:
		panic(ErrInvalidKDF)
	}
}

// Extract derives a pseudorandom key from a high-entropy, secret input and a
// salt. The size of the output is determined by KDF.ExtractSize.
func (k KDF) Extract(secret, salt []byte) (pseudorandomKey []byte) {
	return hkdf.Extract(k.hash(), secret, salt)
}

// Expand derives a variable length pseudorandom string from a pseudorandom key
// and an information string. Panics if the pseudorandom key is less
// than N bytes, or if the output length is greater than 255*N bytes,
// where N is the size returned by KDF.Extract function.
func (k KDF) Expand(pseudorandomKey, info []byte, outputLen uint) []byte {
	extractSize := k.ExtractSize()
	if len(pseudorandomKey) < extractSize {
		panic(fmt.Errorf("pseudorandom key must be %v bytes", extractSize))
	}
	maxLength := uint(255 * extractSize)
	if outputLen > maxLength {
		panic(fmt.Errorf("output length must be less than %v bytes", maxLength))
	}
	output := make([]byte, outputLen)
	rd := hkdf.Expand(k.hash(), pseudorandomKey[:extractSize], info)
	_, err := io.ReadFull(rd, output)
	if err != nil {
		panic(err)
	}
	return output
}

func (k KDF) hash() func() hash.Hash {
	switch k {
	case KDF_HKDF_SHA256:
		return crypto.SHA256.New
	case KDF_HKDF_SHA384:
		return crypto.SHA384.New
	case KDF_HKDF_SHA512:
		return crypto.SHA512.New
	default:
		panic(ErrInvalidKDF)
	}
}

type AEAD uint16

//nolint:golint,stylecheck
const (
	// AEAD_AES128GCM is AES-128 block cipher in Galois Counter Mode (GCM).
	AEAD_AES128GCM AEAD = 0x01
	// AEAD_AES256GCM is AES-256 block cipher in Galois Counter Mode (GCM).
	AEAD_AES256GCM AEAD = 0x02
	// AEAD_ChaCha20Poly1305 is ChaCha20 stream cipher and Poly1305 MAC.
	AEAD_ChaCha20Poly1305 AEAD = 0x03
)

// New instantiates an AEAD cipher from the identifier, returns an error if the
// identifier is not known.
func (a AEAD) New(key []byte) (cipher.AEAD, error) {
	switch a {
	case AEAD_AES128GCM, AEAD_AES256GCM:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case AEAD_ChaCha20Poly1305:
		return chacha20poly1305.New(key)
	default:
		panic(ErrInvalidAEAD)
	}
}

func (a AEAD) IsValid() bool {
	switch a {
	case AEAD_AES128GCM,
		AEAD_AES256GCM,
		AEAD_ChaCha20Poly1305:
		return true
	default:
		return false
	}
}

// KeySize returns the size in bytes of the keys used by the AEAD cipher.
func (a AEAD) KeySize() uint {
	switch a {
	case AEAD_AES128GCM:
		return 16
	case AEAD_AES256GCM:
		return 32
	case AEAD_ChaCha20Poly1305:
		return chacha20poly1305.KeySize
	default:
		panic(ErrInvalidAEAD)
	}
}

// NonceSize returns the size in bytes of the nonce used by the AEAD cipher.
func (a AEAD) NonceSize() uint {
	switch a {
	case AEAD_AES128GCM,
		AEAD_AES256GCM,
		AEAD_ChaCha20Poly1305:
		return 12
	default:
		panic(ErrInvalidAEAD)
	}
}

// CipherLen returns the length of a ciphertext corresponding to a message of
// length mLen.
func (a AEAD) CipherLen(mLen uint) uint {
	switch a {
	case AEAD_AES128GCM, AEAD_AES256GCM, AEAD_ChaCha20Poly1305:
		return mLen + 16
	default:
		panic(ErrInvalidAEAD)
	}
}

var (
	dhkemp256hkdfsha256, dhkemp384hkdfsha384, dhkemp521hkdfsha512 shortKEM
	dhkemx25519hkdfsha256, dhkemx448hkdfsha512                    xKEM
	hybridkemX25519Kyber768                                       hybridKEM
	kemXwing                                                      genericNoAuthKEM
)

func init() {
	dhkemp256hkdfsha256.Curve = ecdh.P256()
	dhkemp256hkdfsha256.dhKemBase.id = KEM_P256_HKDF_SHA256
	dhkemp256hkdfsha256.dhKemBase.name = "HPKE_KEM_P256_HKDF_SHA256"
	dhkemp256hkdfsha256.dhKemBase.Hash = crypto.SHA256
	dhkemp256hkdfsha256.dhKemBase.dhKEM = dhkemp256hkdfsha256

	dhkemp384hkdfsha384.Curve = ecdh.P384()
	dhkemp384hkdfsha384.dhKemBase.id = KEM_P384_HKDF_SHA384
	dhkemp384hkdfsha384.dhKemBase.name = "HPKE_KEM_P384_HKDF_SHA384"
	dhkemp384hkdfsha384.dhKemBase.Hash = crypto.SHA384
	dhkemp384hkdfsha384.dhKemBase.dhKEM = dhkemp384hkdfsha384

	dhkemp521hkdfsha512.Curve = ecdh.P521()
	dhkemp521hkdfsha512.dhKemBase.id = KEM_P521_HKDF_SHA512
	dhkemp521hkdfsha512.dhKemBase.name = "HPKE_KEM_P521_HKDF_SHA512"
	dhkemp521hkdfsha512.dhKemBase.Hash = crypto.SHA512
	dhkemp521hkdfsha512.dhKemBase.dhKEM = dhkemp521hkdfsha512

	dhkemx25519hkdfsha256.size = x25519.Size
	dhkemx25519hkdfsha256.dhKemBase.id = KEM_X25519_HKDF_SHA256
	dhkemx25519hkdfsha256.dhKemBase.name = "HPKE_KEM_X25519_HKDF_SHA256"
	dhkemx25519hkdfsha256.dhKemBase.Hash = crypto.SHA256
	dhkemx25519hkdfsha256.dhKemBase.dhKEM = dhkemx25519hkdfsha256

	dhkemx448hkdfsha512.size = x448.Size
	dhkemx448hkdfsha512.dhKemBase.id = KEM_X448_HKDF_SHA512
	dhkemx448hkdfsha512.dhKemBase.name = "HPKE_KEM_X448_HKDF_SHA512"
	dhkemx448hkdfsha512.dhKemBase.Hash = crypto.SHA512
	dhkemx448hkdfsha512.dhKemBase.dhKEM = dhkemx448hkdfsha512

	hybridkemX25519Kyber768.kemBase.id = KEM_X25519_KYBER768_DRAFT00
	hybridkemX25519Kyber768.kemBase.name = "HPKE_KEM_X25519_KYBER768_HKDF_SHA256"
	hybridkemX25519Kyber768.kemBase.Hash = crypto.SHA256
	hybridkemX25519Kyber768.kemA = dhkemx25519hkdfsha256
	hybridkemX25519Kyber768.kemB = kyber768.Scheme()

	kemXwing.Scheme = xwing.Scheme()
	kemXwing.name = "HPKE_KEM_XWING"
}
