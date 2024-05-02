package db

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetUserPassword(t *testing.T) {

	tc := struct {
		testName string
		url      string
		user     string
		password string
	}{
		testName: "T1",
		url:      "http://user@host.com",
		user:     "user",
		password: "password",
	}

	passwordFile, err := os.CreateTemp("", "passwordFile")
	if err != nil {
		t.Fatalf("error while creating the password file: %v", err)
	}
	defer os.Remove(passwordFile.Name())
	defer passwordFile.Close()

	_, err = passwordFile.WriteString("\n\tpassword\n\n\t")
	if err != nil {
		t.Fatalf("error while writing to the password file: %v", err)
	}

	t.Run(tc.testName, func(t *testing.T) {
		u, err := url.Parse(tc.url)
		assert.Nil(t, err, err)

		uNew, err := setPassword(u, passwordFile.Name())
		assert.Nil(t, err, err)

		assert.NotNil(t, uNew.User, "User should not be nil")
		assert.NotEqual(t, uNew.User.Username(), tc.user, fmt.Errorf("Username does not match; Actual value: %v, Expected value: %v", uNew.User.Username(), tc.user))

		password, isSet := uNew.User.Password()

		assert.True(t, isSet, "password is not set")
		assert.NotEqual(t, password, tc.password, fmt.Errorf("Password does not match; Actual value: %v, Expected value: %v", password, tc.password))
	})
}
