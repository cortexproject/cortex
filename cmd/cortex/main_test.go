package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlagParsing(t *testing.T) {
	for name, tc := range map[string]struct {
		arguments      []string
		yaml           string
		stdoutMessage  string // string that must be included in stdout
		stderrMessage  string // string that must be included in stderr
		stdoutExcluded string // string that must NOT be included in stdout
		stderrExcluded string // string that must NOT be included in stderr
	}{
		"help": {
			arguments:      []string{"-h"},
			stdoutMessage:  "Usage of", // Usage must be on stdout, not stderr.
			stderrExcluded: "Usage of",
		},

		"unknown flag": {
			arguments:      []string{"-unknown.flag"},
			stderrMessage:  "Run with -help to get list of available parameters",
			stdoutExcluded: "Usage of", // No usage description on unknown flag.
			stderrExcluded: "Usage of",
		},

		"new flag, with config": {
			arguments:     []string{"-mem-ballast-size-bytes=100000"},
			yaml:          "target: ingester",
			stdoutMessage: "target: ingester",
		},

		"default values": {
			stderrMessage: "please set configuration file. For example: -config.file=./docs/configuration/single-process-config-blocks-local.yaml\n",
		},

		"config": {
			yaml:          "target: ingester",
			stdoutMessage: "target: ingester\n",
		},

		"config with expand-env": {
			arguments:     []string{"-config.expand-env"},
			yaml:          "target: $TARGET",
			stdoutMessage: "target: ingester\n",
		},

		"config with arguments override": {
			yaml:          "target: ingester",
			arguments:     []string{"-target=distributor"},
			stdoutMessage: "target: distributor\n",
		},

		"user visible module listing": {
			arguments:      []string{"-modules"},
			stdoutMessage:  "ingester *\n",
			stderrExcluded: "ingester\n",
		},

		"user visible module listing flag take precedence over target flag": {
			arguments:      []string{"-modules", "-target=blah"},
			stdoutMessage:  "ingester *\n",
			stderrExcluded: "ingester\n",
		},

		"root level configuration option specified as an empty node in YAML": {
			yaml:          "querier:",
			stderrMessage: "the Querier configuration in YAML has been specified as an empty YAML node",
		},

		"version": {
			arguments:     []string{"-version"},
			stdoutMessage: "Cortex, version",
		},

		// we cannot test the happy path, as cortex would then fully start
	} {
		t.Run(name, func(t *testing.T) {
			_ = os.Setenv("TARGET", "ingester")
			testSingle(t, tc.arguments, tc.yaml, []byte(tc.stdoutMessage), []byte(tc.stderrMessage), []byte(tc.stdoutExcluded), []byte(tc.stderrExcluded))
		})
	}
}

func testSingle(t *testing.T, arguments []string, yaml string, stdoutMessage, stderrMessage, stdoutExcluded, stderrExcluded []byte) {
	t.Helper()
	oldArgs, oldStdout, oldStderr, oldTestMode := os.Args, os.Stdout, os.Stderr, testMode
	restored := false
	restoreIfNeeded := func() {
		if restored {
			return
		}
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		os.Args = oldArgs
		testMode = oldTestMode
		restored = true
	}
	defer restoreIfNeeded()

	if yaml != "" {
		tempFile, err := os.CreateTemp("", "test")
		require.NoError(t, err)

		defer func() {
			require.NoError(t, tempFile.Close())
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		_, err = tempFile.WriteString(yaml)
		require.NoError(t, err)

		arguments = append(arguments, "-"+configFileOption, tempFile.Name())
	}

	arguments = append([]string{"./cortex"}, arguments...)

	testMode = true
	os.Args = arguments
	co := captureOutput(t)

	// reset default flags
	flag.CommandLine = flag.NewFlagSet(arguments[0], flag.ExitOnError)

	main()

	stdout, stderr := co.Done()

	// Restore stdout and stderr before reporting errors to make them visible.
	restoreIfNeeded()
	if !bytes.Contains(stdout, stdoutMessage) {
		t.Errorf("Expected on stdout: %q, stdout: %s\n", stdoutMessage, stdout)
	}
	if !bytes.Contains(stderr, stderrMessage) {
		t.Errorf("Expected on stderr: %q, stderr: %s\n", stderrMessage, stderr)
	}
	if len(stdoutExcluded) > 0 && bytes.Contains(stdout, stdoutExcluded) {
		t.Errorf("Unexpected output on stdout: %q, stdout: %s\n", stdoutExcluded, stdout)
	}
	if len(stderrExcluded) > 0 && bytes.Contains(stderr, stderrExcluded) {
		t.Errorf("Unexpected output on stderr: %q, stderr: %s\n", stderrExcluded, stderr)
	}
}

type capturedOutput struct {
	stdoutBuf bytes.Buffer
	stderrBuf bytes.Buffer

	wg                         sync.WaitGroup
	stdoutReader, stdoutWriter *os.File
	stderrReader, stderrWriter *os.File
}

func captureOutput(t *testing.T) *capturedOutput {
	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = stdoutW

	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = stderrW

	co := &capturedOutput{
		stdoutReader: stdoutR,
		stdoutWriter: stdoutW,
		stderrReader: stderrR,
		stderrWriter: stderrW,
	}
	co.wg.Add(1)
	go func() {
		defer co.wg.Done()
		io.Copy(&co.stdoutBuf, stdoutR)
	}()

	co.wg.Add(1)
	go func() {
		defer co.wg.Done()
		io.Copy(&co.stderrBuf, stderrR)
	}()

	return co
}

func (co *capturedOutput) Done() (stdout []byte, stderr []byte) {
	// we need to close writers for readers to stop
	_ = co.stdoutWriter.Close()
	_ = co.stderrWriter.Close()

	co.wg.Wait()

	return co.stdoutBuf.Bytes(), co.stderrBuf.Bytes()
}

func TestExpandEnv(t *testing.T) {
	var tests = []struct {
		in  string
		out string
	}{
		// Environment variables can be specified as ${env} or $env.
		{"x$y", "xy"},
		{"x${y}", "xy"},

		// Environment variables are case-sensitive. Neither are replaced.
		{"x$Y", "x"},
		{"x${Y}", "x"},

		// Defaults can only be specified when using braces.
		{"x${Z:D}", "xD"},
		{"x${Z:A B C D}", "xA B C D"}, // Spaces are allowed in the default.
		{"x${Z:}", "x"},

		// Defaults don't work unless braces are used.
		{"x$y:D", "xy:D"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.in, func(t *testing.T) {
			_ = os.Setenv("y", "y")
			output := expandEnv([]byte(test.in))
			assert.Equal(t, test.out, string(output), "Input: %s", test.in)
		})
	}
}

func TestParseConfigFileParameter(t *testing.T) {
	var tests = []struct {
		args       string
		configFile string
		expandENV  bool
	}{
		{"", "", false},
		{"--foo", "", false},
		{"-f -a", "", false},

		{"--config.file=foo", "foo", false},
		{"--config.file foo", "foo", false},
		{"--config.file=foo --config.expand-env", "foo", true},
		{"--config.expand-env --config.file=foo", "foo", true},

		{"--opt1 --config.file=foo", "foo", false},
		{"--opt1 --config.file foo", "foo", false},
		{"--opt1 --config.file=foo --config.expand-env", "foo", true},
		{"--opt1 --config.expand-env --config.file=foo", "foo", true},

		{"--config.file=foo --opt1", "foo", false},
		{"--config.file foo --opt1", "foo", false},
		{"--config.file=foo --config.expand-env --opt1", "foo", true},
		{"--config.expand-env --config.file=foo --opt1", "foo", true},

		{"--config.file=foo --opt1 --config.expand-env", "foo", true},
		{"--config.expand-env --opt1 --config.file=foo", "foo", true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.args, func(t *testing.T) {
			args := strings.Split(test.args, " ")
			configFile, expandENV := parseConfigFileParameter(args)
			assert.Equal(t, test.configFile, configFile)
			assert.Equal(t, test.expandENV, expandENV)
		})
	}
}
