//go:build requires_docker
// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"path/filepath"

	"github.com/cortexproject/cortex/integration/ca"
)

func writeCerts(dir string, dnsNames ...string) error {
	// set the ca
	cert := ca.New("Test")

	// Ensure the entire path of directories exist.
	if err := os.MkdirAll(filepath.Join(dir, "certs"), os.ModePerm); err != nil {
		return err
	}

	if err := cert.WriteCACertificate(filepath.Join(dir, caCertFile)); err != nil {
		return err
	}

	// server certificate
	if err := cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		filepath.Join(dir, clientCertFile),
		filepath.Join(dir, clientKeyFile),
	); err != nil {
		return err
	}
	if err := cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server"},
			DNSNames:    dnsNames,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		filepath.Join(dir, serverCertFile),
		filepath.Join(dir, serverKeyFile),
	); err != nil {
		return err
	}
	return nil
}
