package db

import (
	"net/url"
	"os"
	"testing"
)

func TestUserPasswordFromPasswordFile(t *testing.T) {

	tempFile, _ := os.CreateTemp("", "testpassword")
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

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

			updatedURL, err := SetUserPassword(u, tempFile.Name())
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if u.User == nil || updatedURL.User.Username() != tc.expectedUsername {
				t.Errorf("Username not set as expected. Got: %v, Expected: %v", updatedURL.User.Username(), tc.expectedUsername)
			}

			if trimmedPassword, _ := updatedURL.User.Password(); trimmedPassword != tc.expectedPassword {
				t.Errorf("Password not set as expected. Got: %v, Expected: %v", trimmedPassword, tc.expectedPassword)
			}
		})
	}
}