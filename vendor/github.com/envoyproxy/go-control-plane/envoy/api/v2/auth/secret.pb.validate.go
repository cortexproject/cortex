// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: envoy/api/v2/auth/secret.proto

package envoy_api_v2_auth

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
	_ = sort.Sort
)

// Validate checks the field values on GenericSecret with the rules defined in
// the proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *GenericSecret) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on GenericSecret with the rules defined
// in the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in GenericSecretMultiError, or
// nil if none found.
func (m *GenericSecret) ValidateAll() error {
	return m.validate(true)
}

func (m *GenericSecret) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetSecret()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, GenericSecretValidationError{
					field:  "Secret",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, GenericSecretValidationError{
					field:  "Secret",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSecret()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return GenericSecretValidationError{
				field:  "Secret",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return GenericSecretMultiError(errors)
	}
	return nil
}

// GenericSecretMultiError is an error wrapping multiple validation errors
// returned by GenericSecret.ValidateAll() if the designated constraints
// aren't met.
type GenericSecretMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m GenericSecretMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m GenericSecretMultiError) AllErrors() []error { return m }

// GenericSecretValidationError is the validation error returned by
// GenericSecret.Validate if the designated constraints aren't met.
type GenericSecretValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e GenericSecretValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e GenericSecretValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e GenericSecretValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e GenericSecretValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e GenericSecretValidationError) ErrorName() string { return "GenericSecretValidationError" }

// Error satisfies the builtin error interface
func (e GenericSecretValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sGenericSecret.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = GenericSecretValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = GenericSecretValidationError{}

// Validate checks the field values on SdsSecretConfig with the rules defined
// in the proto definition for this message. If any rules are violated, the
// first error encountered is returned, or nil if there are no violations.
func (m *SdsSecretConfig) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on SdsSecretConfig with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// SdsSecretConfigMultiError, or nil if none found.
func (m *SdsSecretConfig) ValidateAll() error {
	return m.validate(true)
}

func (m *SdsSecretConfig) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Name

	if all {
		switch v := interface{}(m.GetSdsConfig()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, SdsSecretConfigValidationError{
					field:  "SdsConfig",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, SdsSecretConfigValidationError{
					field:  "SdsConfig",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSdsConfig()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return SdsSecretConfigValidationError{
				field:  "SdsConfig",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return SdsSecretConfigMultiError(errors)
	}
	return nil
}

// SdsSecretConfigMultiError is an error wrapping multiple validation errors
// returned by SdsSecretConfig.ValidateAll() if the designated constraints
// aren't met.
type SdsSecretConfigMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m SdsSecretConfigMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m SdsSecretConfigMultiError) AllErrors() []error { return m }

// SdsSecretConfigValidationError is the validation error returned by
// SdsSecretConfig.Validate if the designated constraints aren't met.
type SdsSecretConfigValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SdsSecretConfigValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SdsSecretConfigValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SdsSecretConfigValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SdsSecretConfigValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SdsSecretConfigValidationError) ErrorName() string { return "SdsSecretConfigValidationError" }

// Error satisfies the builtin error interface
func (e SdsSecretConfigValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSdsSecretConfig.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SdsSecretConfigValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SdsSecretConfigValidationError{}

// Validate checks the field values on Secret with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Secret) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Secret with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in SecretMultiError, or nil if none found.
func (m *Secret) ValidateAll() error {
	return m.validate(true)
}

func (m *Secret) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Name

	switch m.Type.(type) {

	case *Secret_TlsCertificate:

		if all {
			switch v := interface{}(m.GetTlsCertificate()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "TlsCertificate",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "TlsCertificate",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetTlsCertificate()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return SecretValidationError{
					field:  "TlsCertificate",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *Secret_SessionTicketKeys:

		if all {
			switch v := interface{}(m.GetSessionTicketKeys()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "SessionTicketKeys",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "SessionTicketKeys",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetSessionTicketKeys()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return SecretValidationError{
					field:  "SessionTicketKeys",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *Secret_ValidationContext:

		if all {
			switch v := interface{}(m.GetValidationContext()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "ValidationContext",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "ValidationContext",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetValidationContext()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return SecretValidationError{
					field:  "ValidationContext",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *Secret_GenericSecret:

		if all {
			switch v := interface{}(m.GetGenericSecret()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "GenericSecret",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, SecretValidationError{
						field:  "GenericSecret",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetGenericSecret()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return SecretValidationError{
					field:  "GenericSecret",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if len(errors) > 0 {
		return SecretMultiError(errors)
	}
	return nil
}

// SecretMultiError is an error wrapping multiple validation errors returned by
// Secret.ValidateAll() if the designated constraints aren't met.
type SecretMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m SecretMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m SecretMultiError) AllErrors() []error { return m }

// SecretValidationError is the validation error returned by Secret.Validate if
// the designated constraints aren't met.
type SecretValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SecretValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SecretValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SecretValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SecretValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SecretValidationError) ErrorName() string { return "SecretValidationError" }

// Error satisfies the builtin error interface
func (e SecretValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSecret.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SecretValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SecretValidationError{}
