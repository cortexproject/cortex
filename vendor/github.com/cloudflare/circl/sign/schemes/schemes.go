// Package schemes contains a register of signature algorithms.
//
// Implemented schemes:
//
//	Ed25519
//	Ed448
//	Ed25519-Dilithium2
//	Ed448-Dilithium3
package schemes

import (
	"strings"

	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cloudflare/circl/sign/ed448"
	"github.com/cloudflare/circl/sign/eddilithium2"
	"github.com/cloudflare/circl/sign/eddilithium3"

	dilithium2 "github.com/cloudflare/circl/sign/dilithium/mode2"
	dilithium3 "github.com/cloudflare/circl/sign/dilithium/mode3"
	dilithium5 "github.com/cloudflare/circl/sign/dilithium/mode5"
	"github.com/cloudflare/circl/sign/mldsa/mldsa44"
	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
)

var allSchemes = [...]sign.Scheme{
	ed25519.Scheme(),
	ed448.Scheme(),
	eddilithium2.Scheme(),
	eddilithium3.Scheme(),
	dilithium2.Scheme(),
	dilithium3.Scheme(),
	dilithium5.Scheme(),
	mldsa44.Scheme(),
	mldsa65.Scheme(),
	mldsa87.Scheme(),
}

var allSchemeNames map[string]sign.Scheme

func init() {
	allSchemeNames = make(map[string]sign.Scheme)
	for _, scheme := range allSchemes {
		allSchemeNames[strings.ToLower(scheme.Name())] = scheme
	}
}

// ByName returns the scheme with the given name and nil if it is not
// supported.
//
// Names are case insensitive.
func ByName(name string) sign.Scheme {
	return allSchemeNames[strings.ToLower(name)]
}

// All returns all signature schemes supported.
func All() []sign.Scheme { a := allSchemes; return a[:] }
