package autorest

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"testing"

	"github.com/Masterminds/semver"
)

func TestVersion(t *testing.T) {
	var declaredVersion *semver.Version
	if temp, err := semver.NewVersion(Version()); nil == err {
		declaredVersion = temp
	} else {
		t.Error(err)
		t.FailNow()
	}
	t.Logf("Declared Version: %s", declaredVersion.String())

	var currentVersion *semver.Version
	if temp, err := getMaxReleasedVersion(); nil == err {
		currentVersion = temp
	} else {
		t.Error(err)
		t.FailNow()
	}
	t.Logf("Current Release Version: %s", currentVersion.String())

	if !declaredVersion.GreaterThan(currentVersion) {
		t.Log("autorest: Assertion that the Declared version is greater than Current Release Version failed", currentVersion.String(), declaredVersion.String())
		t.Fail()
	}
}

// Variables required by getMaxReleasedVersion. None of these variables should be used outside of that
// function.
var (
	maxReleasedVersion *semver.Version
)

func getMaxReleasedVersion() (*semver.Version, error) {
	if maxReleasedVersion == nil {
		var wg sync.WaitGroup
		var currentTag string
		var emptyVersion semver.Version
		reader, writer := io.Pipe()

		wg.Add(2)

		var tagFetchErr error
		go func() {
			defer wg.Done()
			defer writer.Close()

			tagLister := exec.Command("git", "tag")
			tagLister.Stdout = writer

			if err := tagLister.Run(); err != nil {
				tagFetchErr = err
			}
		}()

		var tagReadErr error
		go func() {
			defer wg.Done()
			defer reader.Close()

			maxReleasedVersion = &emptyVersion
			for {
				if parity, err := fmt.Fscanln(reader, &currentTag); err == io.EOF {
					break
				} else if err != nil {
					tagReadErr = err
					return
				} else if parity != 1 {
					tagReadErr = errors.New("unexpected format")
					return
				}

				if currentVersion, err := semver.NewVersion(currentTag); err == nil && currentVersion.GreaterThan(maxReleasedVersion) {
					maxReleasedVersion = currentVersion
				}
			}
		}()

		wg.Wait()

		if tagFetchErr != nil {
			maxReleasedVersion = nil
			return nil, tagFetchErr
		}

		if tagReadErr != nil {
			maxReleasedVersion = nil
			return nil, tagReadErr
		}
	}
	return maxReleasedVersion, nil
}
