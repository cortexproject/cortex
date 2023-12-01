//go:build !integration
// +build !integration

package dbtest

import (
	"io/ioutil"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/configs/db"
)

// Setup sets up stuff for testing, creating a new database
func Setup(t *testing.T) db.DB {
	require.NoError(t, logging.Setup("debug"))
	database, err := db.New(db.Config{
		URI: "memory://",
	})
	require.NoError(t, err)
	return database
}

// Cleanup cleans up after a test
func Cleanup(t *testing.T, database db.DB) {
	require.NoError(t, database.Close())
}

func TestUserPasswordFromPasswordFile(t *testing.T) {

	tempFile, _ := ioutil.TempFile("", "testpassword")
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	_, writeErr := tempFile.WriteString("   testpassword   ")
	if writeErr != nil {
		t.Fatalf("Error writing to temporary file: %v", writeErr)
	}

	testCases := []struct {
		testName         string
		url              string
		expectedUsername string
		expectedPassword string
	}{
		{
			testName:         "set username and password correctly from url and password file",
			url:              "testscheme://testuser@testhost.com",
			expectedUsername: "testuser",
			expectedPassword: "testpassword",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			u, _ := url.Parse(tc.url)

			updatedUrl, err := db.SetUserPassword(u, tempFile.Name())
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if u.User == nil || updatedUrl.User.Username() != tc.expectedUsername {
				t.Errorf("Username not set as expected. Got: %v, Expected: %v", updatedUrl.User.Username(), tc.expectedUsername)
			}

			if trimmedPassword, _ := updatedUrl.User.Password(); trimmedPassword != tc.expectedPassword {
				t.Errorf("Password not set as expected. Got: %v, Expected: %v", trimmedPassword, tc.expectedPassword)
			}
		})
	}
}
