package tls

import (
	"errors"
	"fmt"

	"github.com/cloudflare/circl/hpke"
	"golang.org/x/crypto/cryptobyte"
)

type ECHConfigContents struct {
	KeyConfig         HPKEKeyConfig
	MaximumNameLength uint8
	PublicName        []byte
	// Extensions        []TLSExtension // ignored for now
	rawExtensions []byte
}

func UnmarshalECHConfigContents(contents []byte) (ECHConfigContents, error) {
	var (
		contentCryptobyte = cryptobyte.String(contents)
		config            ECHConfigContents
	)

	// Parse KeyConfig
	var t cryptobyte.String
	if !contentCryptobyte.ReadUint8(&config.KeyConfig.ConfigId) ||
		!contentCryptobyte.ReadUint16(&config.KeyConfig.KemId) ||
		!contentCryptobyte.ReadUint16LengthPrefixed(&t) ||
		!t.ReadBytes(&config.KeyConfig.rawPublicKey, len(t)) ||
		!contentCryptobyte.ReadUint16LengthPrefixed(&t) ||
		len(t)%4 != 0 {
		return config, errors.New("error parsing KeyConfig")
	}

	// Parse all CipherSuites in KeyConfig
	config.KeyConfig.CipherSuites = nil
	for !t.Empty() {
		var kdfId, aeadId uint16
		if !t.ReadUint16(&kdfId) || !t.ReadUint16(&aeadId) {
			// This indicates an internal bug.
			panic("internal error while parsing contents.cipher_suites")
		}
		config.KeyConfig.CipherSuites = append(config.KeyConfig.CipherSuites, HPKESymmetricCipherSuite{kdfId, aeadId})
	}

	if !contentCryptobyte.ReadUint8(&config.MaximumNameLength) ||
		!contentCryptobyte.ReadUint8LengthPrefixed(&t) ||
		!t.ReadBytes(&config.PublicName, len(t)) ||
		!contentCryptobyte.ReadUint16LengthPrefixed(&t) ||
		!t.ReadBytes(&config.rawExtensions, len(t)) ||
		!contentCryptobyte.Empty() {
		return config, errors.New("error parsing ECHConfigContents")
	}
	return config, nil
}

func (echcc *ECHConfigContents) ParsePublicKey() error {
	var err error
	kem := hpke.KEM(echcc.KeyConfig.KemId)
	if !kem.IsValid() {
		return errors.New("invalid KEM")
	}
	echcc.KeyConfig.PublicKey, err = kem.Scheme().UnmarshalBinaryPublicKey(echcc.KeyConfig.rawPublicKey)
	if err != nil {
		return fmt.Errorf("error parsing public key: %s", err)
	}
	return nil
}

type ECHConfig struct {
	Version  uint16
	Length   uint16
	Contents ECHConfigContents

	raw []byte
}

// UnmarshalECHConfigs parses a sequence of ECH configurations.
//
// Ported from cloudflare/go
func UnmarshalECHConfigs(raw []byte) ([]ECHConfig, error) {
	var (
		err         error
		config      ECHConfig
		t, contents cryptobyte.String
	)
	configs := make([]ECHConfig, 0)
	s := cryptobyte.String(raw)
	if !s.ReadUint16LengthPrefixed(&t) || !s.Empty() {
		return configs, errors.New("error parsing configs")
	}
	raw = raw[2:]
ConfigsLoop:
	for !t.Empty() {
		l := len(t)
		if !t.ReadUint16(&config.Version) ||
			!t.ReadUint16LengthPrefixed(&contents) {
			return nil, errors.New("error parsing config")
		}
		config.Length = uint16(len(contents))
		n := l - len(t)
		config.raw = raw[:n]
		raw = raw[n:]

		if config.Version != utlsExtensionECH {
			continue ConfigsLoop
		}

		/**** cloudflare/go original ****/
		// if !readConfigContents(&contents, &config) {
		// 	return nil, errors.New("error parsing config contents")
		// }

		config.Contents, err = UnmarshalECHConfigContents(contents)
		if err != nil {
			return nil, fmt.Errorf("error parsing config contents: %s", err)
		}

		/**** cloudflare/go original ****/
		// kem := hpke.KEM(config.kemId)
		// if !kem.IsValid() {
		// 	continue ConfigsLoop
		// }
		// config.pk, err = kem.Scheme().UnmarshalBinaryPublicKey(config.rawPublicKey)
		// if err != nil {
		// 	return nil, fmt.Errorf("error parsing public key: %s", err)
		// }

		config.Contents.ParsePublicKey() // parse the bytes into a public key

		configs = append(configs, config)
	}
	return configs, nil
}
