package main

import (
	"bytes"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	for name, tc := range map[string]struct {
		arguments []string
		yaml      string
		message   string
	}{
		"help": {
			arguments: []string{"-h"},
			message:   configFileOption,
		},

		// check that config file is used
		"config with unknown target": {
			yaml:    "target: unknown",
			message: "unrecognised module name: unknown",
		},

		"unknown flag": {
			arguments: []string{"-unknown.flag"},
			message:   "unknown.flag",
		},

		"config with wrong argument override": {
			yaml:      "target: ingester",
			arguments: []string{"-target=unknown"},
			message:   "unrecognised module name: unknown",
		},

		"default values": {
			message: "target: all\n",
		},

		"config": {
			yaml:    "target: ingester",
			message: "target: ingester\n",
		},

		"config with arguments override": {
			yaml:      "target: ingester",
			arguments: []string{"-target=distributor"},
			message:   "target: distributor\n",
		},

		// we cannot test the happy path, as cortex would then fully start
	} {
		t.Run(name, func(t *testing.T) {
			testSingle(t, tc.arguments, tc.yaml, []byte(tc.message))
		})
	}
}

func testSingle(t *testing.T, arguments []string, yaml string, message []byte) {
	oldArgs, oldStdout, oldStderr := os.Args, os.Stdout, os.Stderr
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		os.Args = oldArgs
	}()

	if yaml != "" {
		tempFile, err := ioutil.TempFile("", "test")
		require.NoError(t, err)

		defer func() {
			require.NoError(t, tempFile.Close())
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		_, err = tempFile.WriteString(yaml)
		require.NoError(t, err)

		arguments = append([]string{"-" + configFileOption, tempFile.Name()}, arguments...)
	}

	arguments = append([]string{"./cortex"}, arguments...)

	testMode = true
	os.Args = arguments
	co := captureOutput(t)

	// reset default flags
	flag.CommandLine = flag.NewFlagSet(arguments[0], flag.ExitOnError)

	main()

	stdout, stderr := co.Done()
	require.True(t, bytes.Contains(stdout, message) || bytes.Contains(stderr, message), "Message expected in output: %q, got stdout: %q and stderr: %q", message, stdout, stderr)
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
		_, _ = io.Copy(&co.stdoutBuf, stdoutR)
	}()

	co.wg.Add(1)
	go func() {
		defer co.wg.Done()
		_, _ = io.Copy(&co.stderrBuf, stderrR)
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
