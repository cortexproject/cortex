package hpke

import (
	"crypto/cipher"
	"fmt"
)

type encdecContext struct {
	// Serialized parameters
	suite              Suite
	sharedSecret       []byte
	secret             []byte
	keyScheduleContext []byte
	exporterSecret     []byte
	key                []byte
	baseNonce          []byte
	sequenceNumber     []byte

	// Operational parameters
	cipher.AEAD
	nonce []byte
}

type (
	sealContext struct{ *encdecContext }
	openContext struct{ *encdecContext }
)

// Export takes a context string exporterContext and a desired length (in
// bytes), and produces a secret derived from the internal exporter secret
// using the corresponding KDF Expand function. It panics if length is
// greater than 255*N bytes, where N is the size (in bytes) of the KDF's
// output.
func (c *encdecContext) Export(exporterContext []byte, length uint) []byte {
	maxLength := uint(255 * c.suite.kdfID.ExtractSize())
	if length > maxLength {
		panic(fmt.Errorf("output length must be lesser than %v bytes", maxLength))
	}
	return c.suite.labeledExpand(c.exporterSecret, []byte("sec"),
		exporterContext, uint16(length))
}

func (c *encdecContext) Suite() Suite {
	return c.suite
}

func (c *encdecContext) calcNonce() []byte {
	for i := range c.baseNonce {
		c.nonce[i] = c.baseNonce[i] ^ c.sequenceNumber[i]
	}
	return c.nonce
}

func (c *encdecContext) increment() error {
	// tests whether the sequence number is all-ones, which prevents an
	// overflow after the increment.
	allOnes := byte(0xFF)
	for i := range c.sequenceNumber {
		allOnes &= c.sequenceNumber[i]
	}
	if allOnes == byte(0xFF) {
		return ErrAEADSeqOverflows
	}

	// performs an increment by 1 and verifies whether the sequence overflows.
	carry := uint(1)
	for i := len(c.sequenceNumber) - 1; i >= 0; i-- {
		sum := uint(c.sequenceNumber[i]) + carry
		carry = sum >> 8
		c.sequenceNumber[i] = byte(sum & 0xFF)
	}
	if carry != 0 {
		return ErrAEADSeqOverflows
	}
	return nil
}

func (c *sealContext) Seal(pt, aad []byte) ([]byte, error) {
	ct := c.AEAD.Seal(nil, c.calcNonce(), pt, aad)
	err := c.increment()
	if err != nil {
		for i := range ct {
			ct[i] = 0
		}
		return nil, err
	}
	return ct, nil
}

func (c *openContext) Open(ct, aad []byte) ([]byte, error) {
	pt, err := c.AEAD.Open(nil, c.calcNonce(), ct, aad)
	if err != nil {
		return nil, err
	}
	err = c.increment()
	if err != nil {
		for i := range pt {
			pt[i] = 0
		}
		return nil, err
	}
	return pt, nil
}
