package frankenstein

import (
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
)

type snappyCompressor struct{}

func (c *snappyCompressor) Do(w io.Writer, p []byte) error {
	sw := snappy.NewWriter(w)
	if _, err := sw.Write(p); err != nil {
		return err
	}
	return sw.Close()
}

func (c *snappyCompressor) Type() string {
	return "snappy"
}

type snappyDecompressor struct{}

func (d *snappyDecompressor) Do(r io.Reader) ([]byte, error) {
	sr := snappy.NewReader(r)
	return ioutil.ReadAll(sr)
}

func (d *snappyDecompressor) Type() string {
	return "snappy"
}
