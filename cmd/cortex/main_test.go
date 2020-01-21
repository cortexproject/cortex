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

func TestFlagParsing(t *testing.T) {
	for name, tc := range map[string]struct {
		arguments     []string
		yaml          string
		stdoutMessage string // string that must be included in stdout
		stderrMessage string // string that must be included in stderr
	}{
		"help": {
			arguments:     []string{"-h"},
			stderrMessage: configFileOption,
		},

		// check that config file is used
		"config with unknown target": {
			yaml:          "target: unknown",
			stderrMessage: "unrecognised module name: unknown",
		},

		"argument with unknown target": {
			arguments:     []string{"-target=unknown"},
			stderrMessage: "unrecognised module name: unknown",
		},

		"unknown flag": {
			arguments:     []string{"-unknown.flag"},
			stderrMessage: "-unknown.flag",
		},

		"config with wrong argument override": {
			yaml:          "target: ingester",
			arguments:     []string{"-target=unknown"},
			stderrMessage: "unrecognised module name: unknown",
		},

		"default values": {
			stdoutMessage: "target: all\n",
		},

		"config": {
			yaml:          "target: ingester",
			stdoutMessage: "target: ingester\n",
		},

		"config with arguments override": {
			yaml:          "target: ingester",
			arguments:     []string{"-target=distributor"},
			stdoutMessage: "target: distributor\n",
		},

		// we cannot test the happy path, as cortex would then fully start
	} {
		t.Run(name, func(t *testing.T) {
			testSingle(t, tc.arguments, tc.yaml, []byte(tc.stdoutMessage), []byte(tc.stderrMessage))
		})
	}
}

func testSingle(t *testing.T, arguments []string, yaml string, stdoutMessage, stderrMessage []byte) {
	oldArgs, oldStdout, oldStderr, oldTestMode := os.Args, os.Stdout, os.Stderr, testMode
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		os.Args = oldArgs
		testMode = oldTestMode
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
	if !bytes.Contains(stdout, stdoutMessage) {
		t.Errorf("Expected on stdout: %q, stdout: %s\n", stdoutMessage, stdout)
	}
	if !bytes.Contains(stderr, stderrMessage) {
		t.Errorf("Expected on stderr: %q, stderr: %s\n", stderrMessage, stderr)
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
