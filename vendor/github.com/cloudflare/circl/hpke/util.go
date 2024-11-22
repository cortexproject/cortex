package hpke

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func (st state) keySchedule(ss, info, psk, pskID []byte) (*encdecContext, error) {
	if err := st.verifyPSKInputs(psk, pskID); err != nil {
		return nil, err
	}

	pskIDHash := st.labeledExtract(nil, []byte("psk_id_hash"), pskID)
	infoHash := st.labeledExtract(nil, []byte("info_hash"), info)
	keySchCtx := append(append(
		[]byte{st.modeID},
		pskIDHash...),
		infoHash...)

	secret := st.labeledExtract(ss, []byte("secret"), psk)

	Nk := uint16(st.aeadID.KeySize())
	key := st.labeledExpand(secret, []byte("key"), keySchCtx, Nk)

	aead, err := st.aeadID.New(key)
	if err != nil {
		return nil, err
	}

	Nn := uint16(aead.NonceSize())
	baseNonce := st.labeledExpand(secret, []byte("base_nonce"), keySchCtx, Nn)
	exporterSecret := st.labeledExpand(
		secret,
		[]byte("exp"),
		keySchCtx,
		uint16(st.kdfID.ExtractSize()),
	)

	return &encdecContext{
		st.Suite,
		ss,
		secret,
		keySchCtx,
		exporterSecret,
		key,
		baseNonce,
		make([]byte, Nn),
		aead,
		make([]byte, Nn),
	}, nil
}

func (st state) verifyPSKInputs(psk, pskID []byte) error {
	gotPSK := psk != nil
	gotPSKID := pskID != nil
	if gotPSK != gotPSKID {
		return errors.New("inconsistent PSK inputs")
	}
	switch st.modeID {
	case modeBase | modeAuth:
		if gotPSK {
			return errors.New("PSK input provided when not needed")
		}
	case modePSK | modeAuthPSK:
		if !gotPSK {
			return errors.New("missing required PSK input")
		}
	}
	return nil
}

// Params returns the codepoints for the algorithms comprising the suite.
func (suite Suite) Params() (KEM, KDF, AEAD) {
	return suite.kemID, suite.kdfID, suite.aeadID
}

func (suite Suite) String() string {
	return fmt.Sprintf(
		"kem_id: %v kdf_id: %v aead_id: %v",
		suite.kemID, suite.kdfID, suite.aeadID,
	)
}

func (suite Suite) getSuiteID() (id [10]byte) {
	id[0], id[1], id[2], id[3] = 'H', 'P', 'K', 'E'
	binary.BigEndian.PutUint16(id[4:6], uint16(suite.kemID))
	binary.BigEndian.PutUint16(id[6:8], uint16(suite.kdfID))
	binary.BigEndian.PutUint16(id[8:10], uint16(suite.aeadID))
	return
}

func (suite Suite) isValid() bool {
	return suite.kemID.IsValid() &&
		suite.kdfID.IsValid() &&
		suite.aeadID.IsValid()
}

func (suite Suite) labeledExtract(salt, label, ikm []byte) []byte {
	suiteID := suite.getSuiteID()
	labeledIKM := append(append(append(append(
		make([]byte, 0, len(versionLabel)+len(suiteID)+len(label)+len(ikm)),
		versionLabel...),
		suiteID[:]...),
		label...),
		ikm...)
	return suite.kdfID.Extract(labeledIKM, salt)
}

func (suite Suite) labeledExpand(prk, label, info []byte, l uint16) []byte {
	suiteID := suite.getSuiteID()
	labeledInfo := make([]byte,
		2, 2+len(versionLabel)+len(suiteID)+len(label)+len(info))
	binary.BigEndian.PutUint16(labeledInfo[0:2], l)
	labeledInfo = append(append(append(append(labeledInfo,
		versionLabel...),
		suiteID[:]...),
		label...),
		info...)
	return suite.kdfID.Expand(prk, labeledInfo, uint(l))
}
