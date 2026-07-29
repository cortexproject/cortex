//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/integration/e2e"
)

var (
	// Expose some utilities from the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags     = e2e.MergeFlags
	generateSeries = e2e.GenerateSeries
)

func getCortexProjectDir() string {
	if dir := os.Getenv("CORTEX_CHECKOUT_DIR"); dir != "" {
		return dir
	}

	// use the git path if available
	dir, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err == nil {
		return string(bytes.TrimSpace(dir))
	}

	return os.Getenv("GOPATH") + "/src/github.com/cortexproject/cortex"
}

// getLatestReleaseImage returns the Cortex image reference for the latest release,
// derived from the VERSION file at the project root.
func getLatestReleaseImage() (string, error) {
	content, err := os.ReadFile(filepath.Join(getCortexProjectDir(), "VERSION"))
	if err != nil {
		return "", errors.Wrap(err, "unable to read VERSION file")
	}

	version := strings.TrimSpace(string(content))
	if version == "" {
		return "", errors.New("VERSION file is empty")
	}

	return fmt.Sprintf("quay.io/cortexproject/cortex:v%s", version), nil
}

func writeFileToSharedDir(s *e2e.Scenario, dst string, content []byte) error {
	dst = filepath.Join(s.SharedDir(), dst)

	// Ensure the entire path of directories exist.
	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(
		dst,
		content,
		os.ModePerm)
}

func copyFileToSharedDir(s *e2e.Scenario, src, dst string) error {
	content, err := os.ReadFile(filepath.Join(getCortexProjectDir(), src))
	if err != nil {
		return errors.Wrapf(err, "unable to read local file %s", src)
	}

	return writeFileToSharedDir(s, dst, content)
}

func getServerTLSFlags() map[string]string {
	return map[string]string{
		"-server.grpc-tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, serverCertFile),
		"-server.grpc-tls-key-path":    filepath.Join(e2e.ContainerSharedDir, serverKeyFile),
		"-server.grpc-tls-client-auth": "RequireAndVerifyClientCert",
		"-server.grpc-tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
	}
}

func getServerHTTPTLSFlags() map[string]string {
	return map[string]string{
		"-server.http-tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, serverCertFile),
		"-server.http-tls-key-path":    filepath.Join(e2e.ContainerSharedDir, serverKeyFile),
		"-server.http-tls-client-auth": "RequireAndVerifyClientCert",
		"-server.http-tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
	}
}

func getClientTLSFlagsWithPrefix(prefix string) map[string]string {
	return getTLSFlagsWithPrefix(prefix, "ingester.client", false)
}

func getTLSFlagsWithPrefix(prefix string, servername string, http bool) map[string]string {
	flags := map[string]string{
		"-" + prefix + ".tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, clientCertFile),
		"-" + prefix + ".tls-key-path":    filepath.Join(e2e.ContainerSharedDir, clientKeyFile),
		"-" + prefix + ".tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
		"-" + prefix + ".tls-server-name": servername,
	}

	if !http {
		flags["-"+prefix+".tls-enabled"] = "true"
	}

	return flags
}
