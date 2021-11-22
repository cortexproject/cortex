// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: envoy/config/cluster/v3/outlier_detection.proto

package envoy_config_cluster_v3

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

// Validate checks the field values on OutlierDetection with the rules defined
// in the proto definition for this message. If any rules are violated, the
// first error encountered is returned, or nil if there are no violations.
func (m *OutlierDetection) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on OutlierDetection with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// OutlierDetectionMultiError, or nil if none found.
func (m *OutlierDetection) ValidateAll() error {
	return m.validate(true)
}

func (m *OutlierDetection) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetConsecutive_5Xx()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "Consecutive_5Xx",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "Consecutive_5Xx",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetConsecutive_5Xx()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "Consecutive_5Xx",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if d := m.GetInterval(); d != nil {
		dur, err := d.AsDuration(), d.CheckValid()
		if err != nil {
			err = OutlierDetectionValidationError{
				field:  "Interval",
				reason: "value is not a valid duration",
				cause:  err,
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		} else {

			gt := time.Duration(0*time.Second + 0*time.Nanosecond)

			if dur <= gt {
				err := OutlierDetectionValidationError{
					field:  "Interval",
					reason: "value must be greater than 0s",
				}
				if !all {
					return err
				}
				errors = append(errors, err)
			}

		}
	}

	if d := m.GetBaseEjectionTime(); d != nil {
		dur, err := d.AsDuration(), d.CheckValid()
		if err != nil {
			err = OutlierDetectionValidationError{
				field:  "BaseEjectionTime",
				reason: "value is not a valid duration",
				cause:  err,
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		} else {

			gt := time.Duration(0*time.Second + 0*time.Nanosecond)

			if dur <= gt {
				err := OutlierDetectionValidationError{
					field:  "BaseEjectionTime",
					reason: "value must be greater than 0s",
				}
				if !all {
					return err
				}
				errors = append(errors, err)
			}

		}
	}

	if wrapper := m.GetMaxEjectionPercent(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "MaxEjectionPercent",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetEnforcingConsecutive_5Xx(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingConsecutive_5Xx",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetEnforcingSuccessRate(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingSuccessRate",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if all {
		switch v := interface{}(m.GetSuccessRateMinimumHosts()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateMinimumHosts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateMinimumHosts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSuccessRateMinimumHosts()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "SuccessRateMinimumHosts",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetSuccessRateRequestVolume()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateRequestVolume",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateRequestVolume",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSuccessRateRequestVolume()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "SuccessRateRequestVolume",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetSuccessRateStdevFactor()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateStdevFactor",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "SuccessRateStdevFactor",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSuccessRateStdevFactor()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "SuccessRateStdevFactor",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetConsecutiveGatewayFailure()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "ConsecutiveGatewayFailure",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "ConsecutiveGatewayFailure",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetConsecutiveGatewayFailure()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "ConsecutiveGatewayFailure",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if wrapper := m.GetEnforcingConsecutiveGatewayFailure(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingConsecutiveGatewayFailure",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	// no validation rules for SplitExternalLocalOriginErrors

	if all {
		switch v := interface{}(m.GetConsecutiveLocalOriginFailure()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "ConsecutiveLocalOriginFailure",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "ConsecutiveLocalOriginFailure",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetConsecutiveLocalOriginFailure()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "ConsecutiveLocalOriginFailure",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if wrapper := m.GetEnforcingConsecutiveLocalOriginFailure(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingConsecutiveLocalOriginFailure",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetEnforcingLocalOriginSuccessRate(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingLocalOriginSuccessRate",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetFailurePercentageThreshold(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "FailurePercentageThreshold",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetEnforcingFailurePercentage(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingFailurePercentage",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if wrapper := m.GetEnforcingFailurePercentageLocalOrigin(); wrapper != nil {

		if wrapper.GetValue() > 100 {
			err := OutlierDetectionValidationError{
				field:  "EnforcingFailurePercentageLocalOrigin",
				reason: "value must be less than or equal to 100",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

	}

	if all {
		switch v := interface{}(m.GetFailurePercentageMinimumHosts()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "FailurePercentageMinimumHosts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "FailurePercentageMinimumHosts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetFailurePercentageMinimumHosts()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "FailurePercentageMinimumHosts",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetFailurePercentageRequestVolume()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "FailurePercentageRequestVolume",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, OutlierDetectionValidationError{
					field:  "FailurePercentageRequestVolume",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetFailurePercentageRequestVolume()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return OutlierDetectionValidationError{
				field:  "FailurePercentageRequestVolume",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if d := m.GetMaxEjectionTime(); d != nil {
		dur, err := d.AsDuration(), d.CheckValid()
		if err != nil {
			err = OutlierDetectionValidationError{
				field:  "MaxEjectionTime",
				reason: "value is not a valid duration",
				cause:  err,
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		} else {

			gt := time.Duration(0*time.Second + 0*time.Nanosecond)

			if dur <= gt {
				err := OutlierDetectionValidationError{
					field:  "MaxEjectionTime",
					reason: "value must be greater than 0s",
				}
				if !all {
					return err
				}
				errors = append(errors, err)
			}

		}
	}

	if len(errors) > 0 {
		return OutlierDetectionMultiError(errors)
	}
	return nil
}

// OutlierDetectionMultiError is an error wrapping multiple validation errors
// returned by OutlierDetection.ValidateAll() if the designated constraints
// aren't met.
type OutlierDetectionMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m OutlierDetectionMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m OutlierDetectionMultiError) AllErrors() []error { return m }

// OutlierDetectionValidationError is the validation error returned by
// OutlierDetection.Validate if the designated constraints aren't met.
type OutlierDetectionValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e OutlierDetectionValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e OutlierDetectionValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e OutlierDetectionValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e OutlierDetectionValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e OutlierDetectionValidationError) ErrorName() string { return "OutlierDetectionValidationError" }

// Error satisfies the builtin error interface
func (e OutlierDetectionValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sOutlierDetection.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = OutlierDetectionValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = OutlierDetectionValidationError{}
