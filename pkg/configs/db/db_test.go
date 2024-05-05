package db

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetPassword(t *testing.T) {

	testCases := []struct {
		testName    string
		url         string
		passwordStr string
		isError     bool
		expected    string
	}{
		{
			testName:    "Test1",
			url:         "scheme://user@host.com",
			passwordStr: "\n\tpassword\n\n\t",
			isError:     false,
			expected:    "scheme://user:password@host.com",
		},
		{
			testName:    "Test2",
			url:         "scheme://host.com",
			passwordStr: "\n\tpassword\n\n\t",
			isError:     true,
			expected:    "--database.password-file requires username in --database.uri",
		},
	}

	for _, tc := range testCases {
		passwordFile, err := os.CreateTemp("", "passwordFile")
		if err != nil {
			t.Fatalf("error while creating the password file: %v", err)
		}

		defer os.Remove(passwordFile.Name())
		defer passwordFile.Close()

		_, err = passwordFile.WriteString(tc.passwordStr)
		if err != nil {
			t.Fatalf("error while writing to the password file: %v", err)
		}

		t.Run(tc.testName, func(t *testing.T) {
			u, _ := url.Parse(tc.url)
			uNew, err := setPassword(u, passwordFile.Name())
			if tc.isError {
				assert.Error(t, err)
				assert.Equal(t, tc.expected, err.Error())
			} else {
				assert.NoError(t, err)
				uExp, _ := url.Parse(tc.expected)
				assert.Equal(t, uNew, uExp)
			}
		})
	}
}
