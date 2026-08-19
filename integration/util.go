//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

// getLatestReleaseImage returns the Cortex image reference for the latest published
// release, derived from the VERSION file at the project root.
//
// Set CORTEX_LATEST_RELEASE_IMAGE to override the resolution entirely.
//
// If you change how this resolves, remember to update the preloading done by GitHub
// Actions too (see .github/workflows/test-build-deploy.yml).
func getLatestReleaseImage() (string, error) {
	if image := os.Getenv("CORTEX_LATEST_RELEASE_IMAGE"); image != "" {
		return image, nil
	}

	content, err := os.ReadFile(filepath.Join(getCortexProjectDir(), "VERSION"))
	if err != nil {
		return "", errors.Wrap(err, "unable to read VERSION file")
	}

	version, err := latestReleaseVersion(strings.TrimSpace(string(content)))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("quay.io/cortexproject/cortex:v%s", version), nil
}

// latestReleaseVersion maps the contents of the VERSION file to a version that has
// actually been published to the container registries.
//
// VERSION does not always name a published release. On a release branch it is bumped to
// the version being prepared (e.g. "1.22.0-rc.0") long before the deploy job publishes
// that tag, and the integration job runs before deploy. So a pre-release version resolves
// to the release preceding it, which is always already published by then:
//
//	1.21.1      -> 1.21.1  (VERSION on master is the last GA, whose image exists)
//	1.22.0-rc.0 -> 1.21.0  (the previous minor always shipped a .0)
//	1.22.2-rc.1 -> 1.22.1  (the preceding patch of the same minor)
func latestReleaseVersion(version string) (string, error) {
	if version == "" {
		return "", errors.New("VERSION file is empty")
	}

	// Anything after the first "-" is a pre-release identifier (e.g. "-rc.0").
	base, preRelease, isPreRelease := strings.Cut(version, "-")
	if !isPreRelease {
		return version, nil
	}

	major, minor, patch, err := parseVersion(base)
	if err != nil {
		return "", errors.Wrapf(err, "unable to resolve the release preceding pre-release version %q", version)
	}

	switch {
	case patch > 0:
		// A patch pre-release: the preceding patch of the same minor is published.
		patch--
	case minor > 0:
		// A minor pre-release: the previous minor's initial release is published. Using
		// .0 rather than its latest patch keeps this derivable from VERSION alone.
		minor--
		patch = 0
	default:
		// A major pre-release (e.g. "2.0.0-rc.0"). The last release of the previous major
		// is not derivable from VERSION, so the maintainer has to say which one it is.
		return "", errors.Errorf("cannot resolve the release preceding major pre-release version %q (base %q, pre-release %q):"+
			" set CORTEX_LATEST_RELEASE_IMAGE to the latest published release image", version, base, preRelease)
	}

	return fmt.Sprintf("%d.%d.%d", major, minor, patch), nil
}

func parseVersion(version string) (major, minor, patch int, err error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return 0, 0, 0, errors.Errorf("expected a major.minor.patch version, got %q", version)
	}

	out := make([]int, len(parts))
	for i, part := range parts {
		if out[i], err = strconv.Atoi(part); err != nil {
			return 0, 0, 0, errors.Wrapf(err, "invalid version %q", version)
		}
		if out[i] < 0 {
			return 0, 0, 0, errors.Errorf("invalid version %q", version)
		}
	}

	return out[0], out[1], out[2], nil
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
