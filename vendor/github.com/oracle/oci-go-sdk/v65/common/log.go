// Copyright (c) 2016, 2018, 2026, Oracle and/or its affiliates.  All rights reserved.
// This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

package common

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// sdkLogger an interface for logging in the SDK
type sdkLogger interface {
	//LogLevel returns the log level of sdkLogger
	LogLevel() int

	//Log logs v with the provided format if the current log level is loglevel
	Log(logLevel int, format string, v ...interface{}) error
}

// noLogging no logging messages
const noLogging = 0

// infoLogging minimal logging messages
const infoLogging = 1

// debugLogging some logging messages
const debugLogging = 2

// verboseLogging all logging messages
const verboseLogging = 3

// DefaultSDKLogger the default implementation of the sdkLogger
type DefaultSDKLogger struct {
	currentLoggingLevel int
	verboseLogger       *log.Logger
	debugLogger         *log.Logger
	infoLogger          *log.Logger
	nullLogger          *log.Logger
}

// defaultLogger is the defaultLogger in the SDK
var defaultLogger sdkLogger
var loggerLock sync.Mutex
var file *os.File

const (
	sensitiveHeaderRedactionValue = "REDACTED"
)

var sensitiveExactHeaderNames = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"opc-obo-token":       {},
	"x-api-key":           {},
	"cookie":              {},
	"set-cookie":          {},
	"security-context":    {},
	"password":            {},
	"passphrase":          {},
}

var sensitiveCredentialHeaderSuffixes = []string{
	"access-token",
	"refresh-token",
	"id-token",
	"security-token",
	"session-token",
	"delegation-token",
	"client-secret",
	"private-key",
}

var (
	headerDelimiterPattern      = regexp.MustCompile(`[-_]+`)
	headerLinePattern           = regexp.MustCompile(`(?im)^(\s*)([^:\r\n]+)(\s*:\s*)(.*)$`)
	jsonStringArrayFieldPattern = regexp.MustCompile(`"((?:[^"\\]|\\.)+)"(\s*:\s*)\[((?:"(?:[^"\\]|\\.)*"\s*,\s*)*"(?:[^"\\]|\\.)*"\s*)\]`)
	jsonStringFieldPattern      = regexp.MustCompile(`"((?:[^"\\]|\\.)+)"(\s*:\s*)"((?:[^"\\]|\\.)*)"`)
	jsonStringValuePattern      = regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`)
)

// initializes the SDK defaultLogger as a defaultLogger
func init() {
	l, _ := NewSDKLogger()
	SetSDKLogger(l)
}

// SetSDKLogger sets the logger used by the sdk
func SetSDKLogger(logger sdkLogger) {
	loggerLock.Lock()
	defaultLogger = logger
	loggerLock.Unlock()
}

// NewSDKLogger creates a defaultSDKLogger
// Debug logging is turned on/off by the presence of the environment variable "OCI_GO_SDK_DEBUG"
// The value of the "OCI_GO_SDK_DEBUG" environment variable controls the logging level.
// "null" outputs no log messages
// "i" or "info" outputs minimal log messages
// "d" or "debug" outputs some logs messages
// "v" or "verbose" outputs all logs messages, including body of requests
func NewSDKLogger() (DefaultSDKLogger, error) {
	logger := DefaultSDKLogger{}

	logger.currentLoggingLevel = noLogging
	logger.verboseLogger = log.New(os.Stderr, "VERBOSE ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	logger.debugLogger = log.New(os.Stderr, "DEBUG ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	logger.infoLogger = log.New(os.Stderr, "INFO ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	logger.nullLogger = log.New(ioutil.Discard, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	configured, isLogEnabled := os.LookupEnv("OCI_GO_SDK_DEBUG")

	// If env variable not present turn logging off
	if !isLogEnabled {
		logger.currentLoggingLevel = noLogging
	} else {
		logOutputModeConfig(logger)

		switch strings.ToLower(configured) {
		case "null":
			logger.currentLoggingLevel = noLogging
			break
		case "i", "info":
			logger.currentLoggingLevel = infoLogging
			break
		case "d", "debug":
			logger.currentLoggingLevel = debugLogging
			break
		//1 here for backwards compatibility
		case "v", "verbose", "1":
			logger.currentLoggingLevel = verboseLogging
			break
		default:
			logger.currentLoggingLevel = infoLogging
		}
		logger.infoLogger.Println("logger level set to: ", logger.currentLoggingLevel)
	}

	return logger, nil
}

func (l DefaultSDKLogger) getLoggerForLevel(logLevel int) *log.Logger {
	if logLevel > l.currentLoggingLevel {
		return l.nullLogger
	}

	switch logLevel {
	case noLogging:
		return l.nullLogger
	case infoLogging:
		return l.infoLogger
	case debugLogging:
		return l.debugLogger
	case verboseLogging:
		return l.verboseLogger
	default:
		return l.nullLogger
	}
}

// Set SDK Log output mode
// Output mode is switched based on environment variable "OCI_GO_SDK_LOG_OUPUT_MODE"
// "file" outputs log to a specific file
// "combine" outputs log to both stderr and specific file
// other unsupported value outputs log to stderr
// output file can be set via environment variable "OCI_GO_SDK_LOG_FILE"
// if this environment variable is not set, a default log file will be created under project root path
func logOutputModeConfig(logger DefaultSDKLogger) {
	logMode, isLogOutputModeEnabled := os.LookupEnv("OCI_GO_SDK_LOG_OUTPUT_MODE")
	if !isLogOutputModeEnabled {
		return
	}
	fileName, isLogFileNameProvided := os.LookupEnv("OCI_GO_SDK_LOG_FILE")
	if !isLogFileNameProvided {
		fileName = fmt.Sprintf("logging_%v%s", time.Now().Unix(), ".log")
	}

	switch strings.ToLower(logMode) {
	case "file", "f":
		file = openLogOutputFile(logger, fileName)
		logger.infoLogger.SetOutput(file)
		logger.debugLogger.SetOutput(file)
		logger.verboseLogger.SetOutput(file)
		break
	case "combine", "c":
		file = openLogOutputFile(logger, fileName)
		wrt := io.MultiWriter(os.Stderr, file)

		logger.infoLogger.SetOutput(wrt)
		logger.debugLogger.SetOutput(wrt)
		logger.verboseLogger.SetOutput(wrt)
		break
	}
}

func openLogOutputFile(logger DefaultSDKLogger, fileName string) *os.File {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		logger.verboseLogger.Fatal(err)
	}
	return file
}

// CloseLogFile close the logging file and return error
func CloseLogFile() error {
	return file.Close()
}

// Normalizes a header name before matching it against sensitive header rules
func normalizeHeaderName(name string) string {
	return headerDelimiterPattern.ReplaceAllString(strings.TrimSpace(strings.ToLower(name)), "-")
}

// Returns true when a header name matches the sensitive header rules
func isSensitiveHeaderName(name string) bool {
	normalized := normalizeHeaderName(name)
	if normalized == "" {
		return false
	}

	// Check against set of exact header names
	if _, ok := sensitiveExactHeaderNames[normalized]; ok {
		return true
	}
	if normalized == "x-token" || strings.HasPrefix(normalized, "x-token-") {
		return true
	}
	if normalized == "x-authorization" || strings.HasPrefix(normalized, "x-authorization-") {
		return true
	}
	if strings.HasPrefix(normalized, "x-key-") {
		return true
	}

	// Check if header matches or ends with suffix
	for _, suffix := range sensitiveCredentialHeaderSuffixes {
		if normalized == suffix || strings.HasSuffix(normalized, "-"+suffix) {
			return true
		}
	}

	return false
}

// RedactSensitiveHeadersForLogs returns a copy of headers with sensitive values replaced for logging
func RedactSensitiveHeadersForLogs(headers http.Header) http.Header {
	if headers == nil {
		return nil
	}

	redacted := make(http.Header, len(headers))
	for name, values := range headers {
		if values == nil {
			redacted[name] = nil
			continue
		}

		copiedValues := append([]string(nil), values...)
		if isSensitiveHeaderName(name) {
			for i := range copiedValues {
				copiedValues[i] = sensitiveHeaderRedactionValue
			}
		}
		redacted[name] = copiedValues
	}

	return redacted
}

// Redacts sensitive values from line-oriented header text
func redactSensitiveHeaderLines(value string) string {
	return headerLinePattern.ReplaceAllStringFunc(value, func(match string) string {
		parts := headerLinePattern.FindStringSubmatch(match)
		if len(parts) < 4 || !isSensitiveHeaderName(parts[2]) {
			return match
		}
		return parts[1] + parts[2] + parts[3] + sensitiveHeaderRedactionValue
	})
}

// Redacts sensitive header values from serialized JSON logger text
func redactSensitiveJsonStringFields(value string) string {
	redactedArrays := jsonStringArrayFieldPattern.ReplaceAllStringFunc(value, func(match string) string {
		parts := jsonStringArrayFieldPattern.FindStringSubmatch(match)
		if len(parts) < 4 || !isSensitiveHeaderName(parts[1]) {
			return match
		}
		return `"` + parts[1] + `"` + parts[2] + `[` +
			jsonStringValuePattern.ReplaceAllString(parts[3], `"`+sensitiveHeaderRedactionValue+`"`) +
			`]`
	})

	return jsonStringFieldPattern.ReplaceAllStringFunc(redactedArrays, func(match string) string {
		parts := jsonStringFieldPattern.FindStringSubmatch(match)
		if len(parts) < 3 || !isSensitiveHeaderName(parts[1]) {
			return match
		}
		return `"` + parts[1] + `"` + parts[2] + `"` + sensitiveHeaderRedactionValue + `"`
	})
}

// RedactSensitiveStringForLogs redacts sensitive header values from logger text
func RedactSensitiveStringForLogs(value string) string {
	return redactSensitiveJsonStringFields(redactSensitiveHeaderLines(value))
}

// LogLevel returns the current debug level
func (l DefaultSDKLogger) LogLevel() int {
	return l.currentLoggingLevel
}

// Log logs v with the provided format if the current log level is loglevel
func (l DefaultSDKLogger) Log(logLevel int, format string, v ...interface{}) error {
	logger := l.getLoggerForLevel(logLevel)
	logger.Output(4, fmt.Sprintf(format, v...))
	return nil
}

// Logln logs v appending a new line at the end
// Deprecated
func Logln(v ...interface{}) {
	m := fmt.Sprint(v...)
	defaultLogger.Log(infoLogging, "%s\n", RedactSensitiveStringForLogs(m))
}

// Logf logs v with the provided format
func Logf(format string, v ...interface{}) {
	defaultLogger.Log(infoLogging, "%s", RedactSensitiveStringForLogs(fmt.Sprintf(format, v...)))
}

// Debugf logs v with the provided format if debug mode is set
func Debugf(format string, v ...interface{}) {
	defaultLogger.Log(debugLogging, "%s", RedactSensitiveStringForLogs(fmt.Sprintf(format, v...)))
}

// Debug logs v if debug mode is set
func Debug(v ...interface{}) {
	m := fmt.Sprint(v...)
	defaultLogger.Log(debugLogging, "%s", RedactSensitiveStringForLogs(m))
}

// Debugln logs v appending a new line if debug mode is set
func Debugln(v ...interface{}) {
	m := fmt.Sprint(v...)
	defaultLogger.Log(debugLogging, "%s\n", RedactSensitiveStringForLogs(m))
}

// IfDebug executes closure if debug is enabled
func IfDebug(fn func()) {
	if defaultLogger.LogLevel() >= debugLogging {
		fn()
	}
}

// IfInfo executes closure if info is enabled
func IfInfo(fn func()) {
	if defaultLogger.LogLevel() >= infoLogging {
		fn()
	}
}
