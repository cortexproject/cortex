package flux

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/influxdata/pkg-config/internal/logutil"
	"github.com/influxdata/pkg-config/internal/modfile"
	"github.com/influxdata/pkg-config/internal/modload"
	"github.com/influxdata/pkg-config/internal/module"
	"go.uber.org/zap"
)

type Target struct {
	OS     string
	Arch   string
	Arm    string
	Static bool
}

func (t Target) String() string {
	s := fmt.Sprintf("%s_%s", t.OS, t.Arch)
	if t.Arm != "" {
		s += "v" + t.Arm
	}
	if t.Static {
		s += "_static"
	}
	return s
}

// Determine the cargo target.
func (t Target) DetermineCargoTarget(logger *zap.Logger) string {
	switch {
	case t.OS == "linux" && t.Arch == "amd64" && t.Static:
		return "x86_64-unknown-linux-musl"
	case t.OS == "linux" && t.Arch == "amd64" && !t.Static:
		return "x86_64-unknown-linux-gnu"
	case t.OS == "linux" && t.Arch == "386" && !t.Static:
		return "i686-unknown-linux-gnu"
	case t.OS == "linux" && t.Arch == "arm" && t.Arm == "6" && !t.Static:
		return "arm-unknown-linux-gnueabihf"
	case t.OS == "linux" && t.Arch == "arm" && t.Arm == "6" && t.Static:
		return "arm-unknown-linux-musleabihf"
	case t.OS == "linux" && t.Arch == "arm" && t.Arm == "7" && !t.Static:
		return "armv7-unknown-linux-gnueabihf"
	case t.OS == "linux" && t.Arch == "arm" && t.Arm == "7" && t.Static:
		return "armv7-unknown-linux-musleabihf"
	case t.OS == "linux" && t.Arch == "arm64" && !t.Static:
		return "aarch64-unknown-linux-gnu"
	case t.OS == "linux" && t.Arch == "arm64" && t.Static:
		return "aarch64-unknown-linux-musl"
	case t.OS == "darwin" && t.Arch == "amd64":
		return "x86_64-apple-darwin"
	case t.OS == "windows" && t.Arch == "amd64":
		return "x86_64-pc-windows-gnu"
	default:
		logger.Warn("Unable to determine cargo target. Using the default.", zap.String("target", t.String()))
		return ""
	}
}

type Library struct {
	Path    string
	Version string
	Dir     string
	Target  Target
}

const modulePath = "github.com/influxdata/flux"

func Configure(ctx context.Context, logger *zap.Logger, static bool) (*Library, error) {
	target, err := getTarget(static)
	if err != nil {
		return nil, err
	}

	modroot := modload.ModRoot()
	logger.Info("Determined module root", zap.String("path", modroot))
	data, err := ioutil.ReadFile(filepath.Join(modroot, "go.mod"))
	if err != nil {
		return nil, err
	}

	module, err := modfile.Parse(modroot, data, nil)
	if err != nil {
		return nil, err
	}

	ver, dir, err := findModule(module, logger)
	if err != nil {
		return nil, err
	}
	return &Library{
		Path:    ver.Path,
		Version: ver.Version,
		Dir:     dir,
		Target:  target,
	}, nil
}

func (l *Library) Install(ctx context.Context, logger *zap.Logger) (string, error) {
	// Find the go cache as this is a safe place for us to write files.
	cache, err := getGoCache()
	if err != nil {
		return "", err
	}

	// If the sources are read only (so we can't write build products
	// to the same directory), copy the sources to another location.
	if err := l.copyIfReadOnly(ctx, logger, cache); err != nil {
		return "", err
	}

	targetdir, err := l.build(ctx, logger)
	if err != nil {
		return "", err
	}

	libdir := filepath.Join(cache, "pkgconfig", l.Target.String(), "lib")
	logger.Info("Creating libdir", zap.String("libdir", libdir))
	if err := os.MkdirAll(libdir, 0755); err != nil {
		return "", err
	}

	libnames := []string{"flux"}
	buildid, err := l.determineBuildId(targetdir, libnames)
	if err != nil {
		return "", err
	}

	for _, name := range libnames {
		basename := fmt.Sprintf("lib%s.a", name)
		src := filepath.Join(targetdir, basename)
		dst := filepath.Join(libdir, fmt.Sprintf("lib%s-%s.a", name, buildid))
		logger.Info("Linking library to libdir", zap.String("src", src), zap.String("dst", dst))
		if _, err := os.Stat(dst); err == nil {
			_ = os.Remove(dst)
		}

		if err := safeLink(src, dst); err != nil {
			logger.Error("Could not link library", zap.Error(err))
			return "", err
		}
	}
	return buildid, nil
}

func (l *Library) determineBuildId(targetdir string, libnames []string) (string, error) {
	shasum := sha256.New()
	for _, name := range libnames {
		basename := fmt.Sprintf("lib%s.a", name)
		src := filepath.Join(targetdir, basename)
		data, err := ioutil.ReadFile(src)
		if err != nil {
			return "", err
		}
		shasum.Write(data)
	}
	return hex.EncodeToString(shasum.Sum(nil)), nil
}

// copyIfReadOnly will copy the module to another location if the directory is read only.
func (l *Library) copyIfReadOnly(ctx context.Context, logger *zap.Logger, cache string) error {
	if st, err := os.Stat(l.Dir); err != nil {
		return err
	} else if st.Mode()&0200 != 0 {
		return nil
	}

	// Determine the source path. If the directory already exists,
	// then we have already copied the files.
	srcdir := filepath.Join(cache, "pkgconfig", l.Path+"@"+l.Version)
	if _, err := os.Stat(srcdir); err == nil {
		l.Dir = srcdir
		return nil
	}

	// Copy over the directory.
	if err := filepath.Walk(l.Dir, func(path string, info os.FileInfo, err error) error {
		relpath, err := filepath.Rel(l.Dir, path)
		if err != nil {
			return err
		}

		targetpath := filepath.Join(srcdir, relpath)
		if info.IsDir() {
			return os.MkdirAll(targetpath, 0755)
		}

		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = r.Close() }()

		w, err := os.Create(targetpath)
		if err != nil {
			return err
		}

		if _, err := io.Copy(w, r); err != nil {
			return err
		}
		return w.Close()
	}); err != nil {
		return err
	}

	l.Dir = srcdir
	return nil
}

func (l *Library) build(ctx context.Context, logger *zap.Logger) (string, error) {
	var stderr bytes.Buffer
	cargoCmd := os.Getenv("CARGO")
	if cargoCmd == "" {
		cargoCmd = "cargo"
	}

	cmd := exec.Command(cargoCmd, "build", "--release")
	cmd.Stdout = &stderr
	cmd.Stderr = &stderr
	cmd.Dir = filepath.Join(l.Dir, "libflux")
	cmd.Env = os.Environ()

	targetString := l.Target.DetermineCargoTarget(logger)
	if targetString != "" {
		cmd.Args = append(cmd.Args, "--target", targetString)
	}
	logger.Info("Executing cargo build", zap.String("dir", cmd.Dir), zap.String("target", targetString))
	if err := cmd.Run(); err != nil {
		logutil.LogOutput(&stderr, logger)
		return "", err
	}
	targetDir := filepath.Join(cmd.Dir, "target", targetString, "release")
	logger.Info("Build succeeded", zap.String("dir", targetDir))
	return targetDir, nil
}

func (l *Library) WritePackageConfig(w io.Writer, buildid string) error {
	cache, err := getGoCache()
	if err != nil {
		return err
	}

	var (
		prefix     = filepath.Join(l.Dir, "libflux")
		execPrefix = filepath.Join(cache, "pkgconfig", l.Target.String())
	)
	_, _ = fmt.Fprintf(w, "prefix=%s\n", prefix)
	_, _ = fmt.Fprintf(w, "exec_prefix=%s\n", execPrefix)
	_, _ = fmt.Fprintf(w, "buildid=%s\n", buildid)
	_, _ = io.WriteString(w, `libdir=${exec_prefix}/lib
includedir=${prefix}/include

Name: Flux
`)
	_, _ = fmt.Fprintf(w, "Version: %s\n", l.Version[1:])
	_, _ = fmt.Fprintln(w, `Description: Library for the InfluxData Flux engine`)
	if l.Target.OS == "linux" {
		if l.Target.Static {
			_, _ = fmt.Fprintf(w, "Libs: -L${libdir} -lflux-${buildid} -ldl -lpthread\n")
		} else {
			_, _ = fmt.Fprintf(w, "Libs: -L${libdir} -lflux-${buildid} -ldl\n")
		}
	} else {
		_, _ = fmt.Fprintf(w, "Libs: -L${libdir} -lflux-${buildid}\n")
	}
	_, _ = fmt.Fprintln(w, `Cflags: -I${includedir}`)
	return nil
}

// findModule will find the module in the module file and instantiate
// a module.Version that points to a local copy of the module.
func findModule(mod *modfile.File, logger *zap.Logger) (module.Version, string, error) {
	if mod.Module.Mod.Path == modulePath {
		modroot := modload.ModRoot()
		logger.Info("Flux module is the main module", zap.String("modroot", modroot))
		v, err := getVersion(modroot, logger)
		if err != nil {
			return module.Version{}, "", err
		}
		return module.Version{
			Path:    modulePath,
			Version: v,
		}, modroot, nil
	}

	// Attempt to find the module in the list of replace values.
	for _, replace := range mod.Replace {
		if replace.Old.Path == modulePath {
			// If there is a replacement, and the path to the replacement is a relative path,
			// make the path absolute, relative to the module root.
			if strings.HasPrefix(replace.New.Path, ".") {
				modroot := modload.ModRoot()
				path, err := filepath.Abs(filepath.Join(modroot, replace.New.Path))
				if err != nil {
					return module.Version{}, "", err
				}
				replace.New.Path = path
			}
			return getModule(replace.New, logger)
		}
	}

	// Attempt to find the module in the normal dependencies.
	for _, m := range mod.Require {
		if m.Mod.Path == modulePath {
			return getModule(m.Mod, logger)
		}
	}
	return module.Version{}, "", fmt.Errorf("could not find %s module", modulePath)
}

// getModule will retrieve or copy the module sources to the go build cache.
func getModule(ver module.Version, logger *zap.Logger) (module.Version, string, error) {
	if strings.HasPrefix(ver.Path, "/") || strings.HasPrefix(ver.Path, ".") {
		// We are dealing with a filepath meaning we are building from the filesystem.
		// If this is the case, this is the same as building from the main module.
		// We fill out the version using any git version data and return as-is.
		logger.Info("Module path references the filesystem")
		v, err := getVersion(ver.Path, logger)
		if err != nil {
			return module.Version{}, "", err
		}
		abspath, err := filepath.Abs(ver.Path)
		if err != nil {
			return module.Version{}, "", err
		}
		return module.Version{Version: v}, abspath, nil
	}

	// This references a module. Use go mod download to download the module.
	// We use go mod download specifically to avoid downloading extra dependencies.
	// This should work properly even if vendor was used for the dependencies.
	return downloadModule(logger)
}

// downloadModule will download the module to a file path.
func downloadModule(logger *zap.Logger) (module.Version, string, error) {
	// Download the module and send the JSON output to stdout.
	var stderr bytes.Buffer
	cmd := exec.Command(gocmd, "mod", "download", "-json", modulePath)
	cmd.Stderr = &stderr
	cmd.Dir = modload.ModRoot()
	data, err := cmd.Output()
	if err != nil {
		_ = logutil.LogOutput(&stderr, logger)
		return module.Version{}, "", err
	}

	// Download succeeded. Deserialize the JSON to find the file path.
	var m struct {
		Dir     string
		Path    string
		Version string
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return module.Version{}, "", err
	}
	return module.Version{Path: m.Path, Version: m.Version}, m.Dir, nil
}

func getVersion(dir string, logger *zap.Logger) (string, error) {
	if v, err := getVersionFromPath(dir); err != nil {
		logger.Info("Could not determine version from base path", zap.Error(err))
	} else {
		return v, nil
	}

	if v, err := getVersionFromGit(dir, logger); err != nil {
		logger.Info("Could not determine version from git data", zap.Error(err))
	} else {
		return v, nil
	}
	logger.Info("Using default version")
	return "v0.0.0", nil
}

func getVersionFromPath(dir string) (string, error) {
	reModulePath := regexp.MustCompile(`/github\.com/influxdata\/flux@(v\d+\.\d+\.\d+.*)$`)
	m := reModulePath.FindStringSubmatch(dir)
	if m == nil {
		return "", fmt.Errorf("directory path did not match the module pattern: %s", dir)
	}
	return m[1], nil
}

func getVersionFromGit(dir string, logger *zap.Logger) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.Command("git", "describe")
	cmd.Stderr = &stderr
	cmd.Dir = dir

	out, err := cmd.Output()
	if err != nil {
		_ = logutil.LogOutput(&stderr, logger)
		return "", err
	}
	versionStr := strings.TrimSpace(string(out))

	re := regexp.MustCompile(`(v\d+\.\d+\.\d+)(-.*)?`)
	m := re.FindStringSubmatch(versionStr)
	if m == nil {
		return "", fmt.Errorf("invalid tag version format: %s", versionStr)
	}

	if m[2] == "" {
		return m[1][1:], nil
	}

	v, err := semver.NewVersion(m[1])
	if err != nil {
		return "", err
	}
	*v = v.IncMinor()
	return "v" + v.String(), nil
}

func getGoCache() (string, error) {
	if cacheDir := os.Getenv("GOCACHE"); cacheDir != "" {
		return cacheDir, nil
	}

	cmd := exec.Command(gocmd, "env", "GOCACHE")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func getTarget(static bool) (Target, error) {
	goos := os.Getenv("GOOS")
	if goos == "" {
		cmd := exec.Command(gocmd, "env", "GOOS")
		out, err := cmd.Output()
		if err != nil {
			return Target{}, err
		}
		goos = strings.TrimSpace(string(out))
	}

	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		cmd := exec.Command(gocmd, "env", "GOARCH")
		out, err := cmd.Output()
		if err != nil {
			return Target{}, err
		}
		goarch = strings.TrimSpace(string(out))
	}

	var goarm string
	if goarch == "arm" {
		goarm = os.Getenv("GOARM")
		if goarm == "" {
			cmd := exec.Command(gocmd, "env", "GOARM")
			out, err := cmd.Output()
			if err != nil {
				return Target{}, err
			}
			goarm = strings.TrimSpace(string(out))
		}
	}

	return Target{OS: goos, Arch: goarch, Arm: goarm, Static: static}, nil
}

// safeLink will safely link or copy the file from src to dst.
func safeLink(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}

	srcf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcf.Close() }()

	dstf, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = dstf.Close() }()

	if _, err := io.Copy(dstf, srcf); err != nil {
		return err
	}
	return dstf.Close()
}

// gocmd is the value of environment variable GO if it is non-empty,
// otherwise it is the string "go".
// This allows build scripts to use a particular version of go
// that is not necessarily on the PATH, or not necessarily even named "go".
var gocmd string

func init() {
	if env := os.Getenv("GO"); env == "" {
		gocmd = "go"
	} else {
		gocmd = env
	}
}
