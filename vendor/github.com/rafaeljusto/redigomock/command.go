// Copyright 2014 Rafael Dantas Justo. All rights reserved.
// Use of this source code is governed by a GPL
// license that can be found in the LICENSE file.

package redigomock

import (
	"fmt"
	"reflect"
)

// Response struct that represents single response from `Do` call
type Response struct {
	Response interface{} // Response to send back when this command/arguments are called
	Error    error       // Error to send back when this command/arguments are called
}

// Cmd stores the registered information about a command to return it later
// when request by a command execution
type Cmd struct {
	Name      string        // Name of the command
	Args      []interface{} // Arguments of the command
	Responses []Response    // Slice of returned responses
	Called    bool          // State for this command called or not
}

// cmdHash stores a unique identifier of the command
type cmdHash string

// equal verify if a command/arguments is related to a registered command
func equal(commandName string, args []interface{}, cmd *Cmd) bool {
	if commandName != cmd.Name || len(args) != len(cmd.Args) {
		return false
	}

	for pos := range cmd.Args {
		if implementsFuzzy(cmd.Args[pos]) && implementsFuzzy(args[pos]) {
			if reflect.TypeOf(cmd.Args[pos]) != reflect.TypeOf(args[pos]) {
				return false
			}
		} else if implementsFuzzy(cmd.Args[pos]) || implementsFuzzy(args[pos]) {
			return false
		} else {
			if reflect.DeepEqual(cmd.Args[pos], args[pos]) == false {
				return false
			}
		}
	}
	return true
}

// match check if provided arguments can be matched with any registered
// commands
func match(commandName string, args []interface{}, cmd *Cmd) bool {
	if commandName != cmd.Name || len(args) != len(cmd.Args) {
		return false
	}

	for pos := range cmd.Args {
		if implementsFuzzy(cmd.Args[pos]) {
			if cmd.Args[pos].(FuzzyMatcher).Match(args[pos]) == false {
				return false
			}
		} else if reflect.DeepEqual(cmd.Args[pos], args[pos]) == false {
			return false
		}

	}
	return true
}

// Expect sets a response for this command. Every time a Do or Receive method
// is executed for a registered command this response or error will be
// returned. Expect call returns a pointer to Cmd struct, so you can chain
// Expect calls. Chained responses will be returned on subsequent calls
// matching this commands arguments in FIFO order
func (c *Cmd) Expect(response interface{}) *Cmd {
	c.Responses = append(c.Responses, Response{response, nil})
	return c
}

// ExpectMap works in the same way of the Expect command, but has a key/value
// input to make it easier to build test environments
func (c *Cmd) ExpectMap(response map[string]string) *Cmd {
	var values []interface{}
	for key, value := range response {
		values = append(values, []byte(key))
		values = append(values, []byte(value))
	}
	c.Responses = append(c.Responses, Response{values, nil})
	return c
}

// ExpectError allows you to force an error when executing a
// command/arguments
func (c *Cmd) ExpectError(err error) *Cmd {
	c.Responses = append(c.Responses, Response{nil, err})
	return c
}

// ExpectSlice makes it easier to expect slice value
// e.g - HMGET command
func (c *Cmd) ExpectSlice(resp ...interface{}) *Cmd {
	response := []interface{}{}
	for _, r := range resp {
		response = append(response, r)
	}
	c.Responses = append(c.Responses, Response{response, nil})
	return c
}

// ExpectStringSlice makes it easier to expect a slice of strings, plays nicely
// with redigo.Strings
func (c *Cmd) ExpectStringSlice(resp ...string) *Cmd {
	response := []interface{}{}
	for _, r := range resp {
		response = append(response, []byte(r))
	}
	c.Responses = append(c.Responses, Response{response, nil})
	return c
}

// hash generates a unique identifier for the command
func (c Cmd) hash() cmdHash {
	output := c.Name
	for _, arg := range c.Args {
		output += fmt.Sprintf("%v", arg)
	}
	return cmdHash(output)
}
