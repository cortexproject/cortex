// Package logger provides functions that are logging related
package logger

import (
	"fmt"
)

// Colors which can be used
const (
	YELLOW = "\x1b[33;1m"
	GREEN  = "\x1b[32;1m"
	RED    = "\x1b[31;1m"
	RESET  = "\x1b[33;0m"
)

// Logger struct
type Logger struct {
	Verbosee bool
	Debugg   bool
	NoColor  bool
}

// Debug prints a message when Debugg is set to true on the Logger
func (l Logger) Debug(format string, a ...interface{}) {
	if l.Debugg {
		message := fmt.Sprintf(format, a...)
		Println(message)
	}
}

// Verbose prints a message when Verbosee is set to true on the Logger
func (l Logger) Verbose(format string, a ...interface{}) {
	if l.Verbosee {
		message := fmt.Sprintf(format, a...)
		Println(message)
	}
}

// Warning prints a warning message to Stdout in yellow
func (l Logger) Warning(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	if l.NoColor {
		Println(message)
	} else {
		PrintlnColor(message, YELLOW)
	}
}

// Warning prints a warning message to Stdout in yellow
func Warning(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	PrintlnColor(message, YELLOW)
}

// Output prints a message on Stdout in 'normal' color
func (l Logger) Output(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	Println(message)
}

// Output prints an error message to Stdout in 'normal' color
func Output(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	Println(message)
}

// Error prints an error message to Stdout in red
func (l Logger) Error(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	if l.NoColor {
		Println(message)
	} else {
		PrintlnColor(message, RED)
	}
}

// Error prints an error message to Stdout in red
func Error(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	PrintlnColor(message, RED)
}

// Print prints a message
func Print(message string) {
	fmt.Printf("%s", message)
}

// Println prints a message with a trailing newline
func Println(message string) {
	fmt.Printf("%s\n", message)
}

// PrintColor prints a message in a given ANSI-color
func PrintColor(message string, color string) {
	fmt.Printf("%s%s%s", color, message, RESET)
}

// PrintlnColor prints a message in a given ANSI-color with a trailing newline
func PrintlnColor(message string, color string) {
	fmt.Printf("%s%s%s\n", color, message, RESET)
}
