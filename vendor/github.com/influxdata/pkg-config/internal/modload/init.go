package modload

import (
	"fmt"
	"os"
	"path/filepath"
)

var (
	modRoot     string
	initialized bool
)

func ModRoot() string {
	if !HasModRoot() {
		die("cannot find main module; see 'go help modules'")
	}
	return modRoot
}

func HasModRoot() bool {
	if initialized {
		return modRoot != ""
	}
	initialized = true

	cwd, err := os.Getwd()
	if err != nil {
		die(err.Error())
	}

	for {
		modPath := filepath.Join(cwd, "go.mod")
		if _, err := os.Stat(modPath); err == nil {
			modRoot = cwd
			if vendorRoot, ok := findVendorMod(modRoot); ok {
				modRoot = vendorRoot
			}
			return true
		} else if cwd == "/" {
			return false
		}
		cwd = filepath.Dir(cwd)
	}
}

// findVendorMod will find the module file for the
// project vendoring this dependency if we are inside
// of a vendor directory.
func findVendorMod(cwd string) (string, bool) {
	cwd = filepath.Dir(cwd)
	for cwd != "/" {
		modPath := filepath.Join(cwd, "go.mod")
		if _, err := os.Stat(modPath); err == nil {
			vendorPath := filepath.Join(cwd, "vendor")
			if st, err := os.Stat(vendorPath); err == nil && st.IsDir() {
				return cwd, true
			}
		}
		cwd = filepath.Dir(cwd)
	}
	return "", false
}

func die(msg string) {
	_, _ = fmt.Fprintf(os.Stderr, "modfile: %s\n", msg)
	os.Exit(1)
}
