// Package utils provides functions that are considered utility for being project independent
package utils

import (
	"os"
	"path/filepath"
)

// GetEolChar returns the end of line character used in regexp
// depending on the end_of_line parameter
func GetEolChar(endOfLine string) string {
	switch endOfLine {
	case "lf":
		return "\n"

	case "cr":
		return "\r"
	case "crlf":
		return "\r\n"
	}

	return "\n"
}

// IsRegularFile returns wether a file is a regular file or not
func IsRegularFile(filePath string) bool {
	absolutePath, firstErr := filepath.Abs(filePath)
	fi, secondErr := os.Stat(absolutePath)
	return firstErr == nil && secondErr == nil && fi.Mode().IsRegular()
}

// IsDirectory returns wether a file is a directory or not
func IsDirectory(filePath string) bool {
	absolutePath, firstErr := filepath.Abs(filePath)
	fi, secondErr := os.Stat(absolutePath)
	return firstErr == nil && secondErr == nil && fi.Mode().IsDir()
}

// FileExists returns wether a file exists or not
func FileExists(filePath string) bool {
	absolutePath, _ := filepath.Abs(filePath)
	fi, secondErr := os.Stat(absolutePath)
	return fi != nil && secondErr == nil
}
