/*
 * This file has been modified for use in flux, but the original
 * is part of the grpc-go project.
 *
 * The original can be located here: https://github.com/grpc/grpc-go/blob/master/codes/codes.go
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package codes defines the error codes used by flux.
package codes

import (
	"fmt"
	"strconv"
	"strings"
)

type Code uint32

const (
	// Inherit indicates that this error should inherit the code of the wrapped
	// error. If the wrapped error does not have a code or the error does not
	// have a wrapped error, then this will act the same as Unknown.
	Inherit Code = 0

	// Canceled indicates the operation was canceled (typically by the caller).
	Canceled Code = 1

	// Unknown error. An example of where this error may be returned is
	// if a Status value received from another address space belongs to
	// an error-space that is not known in this address space. Also
	// errors raised by APIs that do not return enough error information
	// may be converted to this error.
	Unknown Code = 2

	// Invalid indicates the client made an invalid request.
	// Note that this differs from FailedPrecondition. It indicates arguments
	// that are missing or problematic regardless of the state of the system
	// (e.g., a missing or invalid argument, a script error, etc).
	Invalid Code = 3

	// DeadlineExceeded means operation expired before completion.
	// For operations that change the state of the system, this error may be
	// returned even if the operation has completed successfully. For
	// example, a successful response from a server could have been delayed
	// long enough for the deadline to expire.
	DeadlineExceeded Code = 4

	// NotFound means some requested entity (e.g., bucket or database) was
	// not found.
	NotFound Code = 5

	// AlreadyExists means an attempt to create an entity failed because one
	// already exists.
	AlreadyExists Code = 6

	// PermissionDenied indicates the caller does not have permission to
	// execute the specified operation. It must not be used for rejections
	// caused by exhausting some resource (use ResourceExhausted
	// instead for those errors). It must not be
	// used if the caller cannot be identified (use Unauthenticated
	// instead for those errors).
	PermissionDenied Code = 7

	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the the memory allocation limit has been
	// reached.
	ResourceExhausted Code = 8

	// FailedPrecondition indicates operation was rejected because the
	// system is not in a state required for the operation's execution.
	// For example, directory to be deleted may be non-empty, an rmdir
	// operation is applied to a non-directory, etc.
	//
	// A litmus test that may help a service implementor in deciding
	// between FailedPrecondition, Aborted, and Unavailable:
	//  (a) Use Unavailable if the client can retry just the failing call.
	//  (b) Use Aborted if the client should retry at a higher-level
	//      (e.g., restarting a read-modify-write sequence).
	//  (c) Use FailedPrecondition if the client should not retry until
	//      the system state has been explicitly fixed. E.g., if an "rmdir"
	//      fails because the directory is non-empty, FailedPrecondition
	//      should be returned since the client should not retry unless
	//      they have first fixed up the directory by deleting files from it.
	//  (d) Use FailedPrecondition if the client performs conditional
	//      REST Get/Update/Delete on a resource and the resource on the
	//      server does not match the condition. E.g., conflicting
	//      read-modify-write on the same resource.
	FailedPrecondition Code = 9

	// Aborted indicates the operation was aborted, typically due to a
	// concurrency issue like sequencer check failures, transaction aborts,
	// etc.
	//
	// See litmus test above for deciding between FailedPrecondition,
	// Aborted, and Unavailable.
	Aborted Code = 10

	// OutOfRange means operation was attempted past the valid range.
	// E.g., accessing a row or column outside of the table dimensions.
	//
	// Unlike Invalid, this error indicates a problem that may
	// be fixed if the system state changes. For example, a 32-bit file
	// system will generate Invalid if asked to read at an
	// offset that is not in the range [0,2^32-1], but it will generate
	// OutOfRange if asked to read from an offset past the current
	// file size.
	OutOfRange Code = 11

	// Unimplemented indicates operation is not implemented or not
	// supported/enabled in this service.
	Unimplemented Code = 12

	// Internal errors. Means some invariants expected by underlying
	// system has been broken. If you see one of these errors,
	// something is very broken.
	//
	// This should not be used when wrapping unidentified errors from
	// other services. Unknown should be used instead when wrapping
	// external errors.
	Internal Code = 13

	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff. Note that it is not always safe to retry
	// non-idempotent operations.
	//
	// See litmus test above for deciding between FailedPrecondition,
	// Aborted, and Unavailable.
	Unavailable Code = 14

	// Unauthenticated indicates the request does not have valid
	// authentication credentials for the operation.
	Unauthenticated Code = 15
)

func (c Code) MarshalText() ([]byte, error) {
	s := c.String()
	return []byte(s), nil
}

var strToCode = map[string]Code{
	"canceled":            Canceled,
	"unknown":             Unknown,
	"invalid":             Invalid,
	"deadline exceeded":   DeadlineExceeded,
	"not found":           NotFound,
	"already exists":      AlreadyExists,
	"permission denied":   PermissionDenied,
	"resource exhausted":  ResourceExhausted,
	"failed precondition": FailedPrecondition,
	"aborted":             Aborted,
	"out of range":        OutOfRange,
	"unimplemented":       Unimplemented,
	"internal":            Internal,
	"unavailable":         Unavailable,
	"unauthenticated":     Unauthenticated,
}

func (c *Code) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		*c = Inherit
		return nil
	}

	code, ok := strToCode[string(text)]
	if ok {
		*c = code
		return nil
	}

	s := string(text)
	if strings.HasPrefix(s, "code(") && strings.HasSuffix(s, ")") {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		*c = Code(n)
		return nil
	}
	return fmt.Errorf("unknown code: %s", text)
}

func (c Code) String() string {
	switch c {
	case Inherit:
		return ""
	case Canceled:
		return "canceled"
	case Unknown:
		return "unknown"
	case Invalid:
		return "invalid"
	case DeadlineExceeded:
		return "deadline exceeded"
	case NotFound:
		return "not found"
	case AlreadyExists:
		return "already exists"
	case PermissionDenied:
		return "permission denied"
	case ResourceExhausted:
		return "resource exhausted"
	case FailedPrecondition:
		return "failed precondition"
	case Aborted:
		return "aborted"
	case OutOfRange:
		return "out of range"
	case Unimplemented:
		return "unimplemented"
	case Internal:
		return "internal"
	case Unavailable:
		return "unavailable"
	case Unauthenticated:
		return "unauthenticated"
	default:
		return "code(" + strconv.FormatInt(int64(c), 10) + ")"
	}
}
