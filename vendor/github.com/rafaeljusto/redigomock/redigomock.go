// Copyright 2014 Rafael Dantas Justo. All rights reserved.
// Use of this source code is governed by a GPL
// license that can be found in the LICENSE file.

package redigomock

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type queueElement struct {
	commandName string
	args        []interface{}
}

type replyElement struct {
	reply interface{}
	err   error
}

// Conn is the struct that can be used where you inject the redigo.Conn on
// your project
type Conn struct {
	SubResponses []Response      // Queue responses for PubSub
	ReceiveWait  bool            // When set to true, Receive method will wait for a value in ReceiveNow channel to proceed, this is useful in a PubSub scenario
	ReceiveNow   chan bool       // Used to lock Receive method to simulate a PubSub scenario
	CloseMock    func() error    // Mock the redigo Close method
	ErrMock      func() error    // Mock the redigo Err method
	FlushMock    func() error    // Mock the redigo Flush method
	commands     []*Cmd          // Slice that stores all registered commands for each connection
	queue        []queueElement  // Slice that stores all queued commands for each connection
	replies      []replyElement  // Slice that stores all queued replies
	stats        map[cmdHash]int // Command calls counter
	statsMut     sync.RWMutex    // Locks the stats so we don't get concurrent map writes
	Errors       []error         // Storage of all error occured in do functions
}

// NewConn returns a new mocked connection. Obviously as we are mocking we
// don't need any Redis connection parameter
func NewConn() *Conn {
	return &Conn{
		ReceiveNow: make(chan bool),
		stats:      make(map[cmdHash]int),
	}
}

// Close can be mocked using the Conn struct attributes
func (c *Conn) Close() error {
	if c.CloseMock == nil {
		return nil
	}

	return c.CloseMock()
}

// Err can be mocked using the Conn struct attributes
func (c *Conn) Err() error {
	if c.ErrMock == nil {
		return nil
	}

	return c.ErrMock()
}

// Command register a command in the mock system using the same arguments of
// a Do or Send commands. It will return a registered command object where
// you can set the response or error
func (c *Conn) Command(commandName string, args ...interface{}) *Cmd {
	cmd := &Cmd{
		Name: commandName,
		Args: args,
	}
	c.removeRelatedCommands(commandName, args)
	c.commands = append(c.commands, cmd)
	return cmd
}

// Script registers a command in the mock system just like Command method
// would do. The first argument is a byte array with the script text, next
// ones are the ones you would pass to redis Script.Do() method
func (c *Conn) Script(scriptData []byte, keyCount int, args ...interface{}) *Cmd {
	h := sha1.New()
	h.Write(scriptData)
	sha1sum := hex.EncodeToString(h.Sum(nil))

	newArgs := make([]interface{}, 2+len(args))
	newArgs[0] = sha1sum
	newArgs[1] = keyCount
	copy(newArgs[2:], args)

	return c.Command("EVALSHA", newArgs...)
}

// GenericCommand register a command without arguments. If a command with
// arguments doesn't match with any registered command, it will look for
// generic commands before throwing an error
func (c *Conn) GenericCommand(commandName string) *Cmd {
	cmd := &Cmd{
		Name: commandName,
	}

	c.removeRelatedCommands(commandName, nil)
	c.commands = append(c.commands, cmd)
	return cmd
}

// find will scan the registered commands, looking for the first command with
// the same name and arguments. If the command is not found nil is returned
func (c *Conn) find(commandName string, args []interface{}) *Cmd {
	for _, cmd := range c.commands {
		if match(commandName, args, cmd) {
			return cmd
		}
	}
	return nil
}

// removeRelatedCommands verify if a command is already registered, removing
// any command already registered with the same name and arguments. This
// should avoid duplicated mocked commands
func (c *Conn) removeRelatedCommands(commandName string, args []interface{}) {
	var unique []*Cmd

	for _, cmd := range c.commands {
		// new array will contain only commands that are not related to the given
		// one
		if !equal(commandName, args, cmd) {
			unique = append(unique, cmd)
		}
	}
	c.commands = unique
}

// Clear removes all registered commands. Useful for connection reuse in test
// scenarios
func (c *Conn) Clear() {
	c.statsMut.Lock()
	defer c.statsMut.Unlock()

	c.commands = []*Cmd{}
	c.queue = []queueElement{}
	c.replies = []replyElement{}
	c.stats = make(map[cmdHash]int)
}

// Do looks in the registered commands (via Command function) if someone
// matches with the given command name and arguments, if so the corresponding
// response or error is returned. If no registered command is found an error
// is returned
func (c *Conn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if commandName == "" {
		if err := c.Flush(); err != nil {
			return nil, err
		}

		if len(c.replies) == 0 {
			return nil, nil
		}

		replies := []interface{}{}
		for _, v := range c.replies {
			if v.err != nil {
				return nil, v.err
			}
			replies = append(replies, v.reply)
		}
		c.replies = []replyElement{}
		return replies, nil
	}

	if len(c.queue) != 0 || len(c.replies) != 0 {
		if err := c.Flush(); err != nil {
			return nil, err
		}
		for _, v := range c.replies {
			if v.err != nil {
				return nil, v.err
			}
		}
		c.replies = []replyElement{}
	}

	return c.do(commandName, args...)
}

func (c *Conn) do(commandName string, args ...interface{}) (reply interface{}, err error) {
	cmd := c.find(commandName, args)
	if cmd == nil {
		// Didn't find a specific command, try to get a generic one
		if cmd = c.find(commandName, nil); cmd == nil {
			var msg string
			for _, regCmd := range c.commands {
				if commandName == regCmd.Name {
					if len(msg) == 0 {
						msg = ". Possible matches are with the arguments:"
					}
					msg += fmt.Sprintf("\n* %#v", regCmd.Args)
				}
			}

			err := fmt.Errorf("command %s with arguments %#v not registered in redigomock library%s",
				commandName, args, msg)
			c.Errors = append(c.Errors, err)
			return nil, err
		}
	}

	c.statsMut.Lock()
	c.stats[cmd.hash()]++
	c.statsMut.Unlock()

	cmd.Called = true
	if len(cmd.Responses) == 0 {
		return nil, nil
	}

	response := cmd.Responses[0]
	if len(cmd.Responses) > 1 {
		cmd.Responses = cmd.Responses[1:]
	}
	return response.Response, response.Error
}

// DoWithTimeout is a helper function for Do call to satisfy the ConnWithTimeout
// interface.
func (c *Conn) DoWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	return c.Do(cmd, args...)
}

// Send stores the command and arguments to be executed later (by the Receive
// function) in a first-come first-served order
func (c *Conn) Send(commandName string, args ...interface{}) error {
	c.queue = append(c.queue, queueElement{
		commandName: commandName,
		args:        args,
	})
	return nil
}

// Flush can be mocked using the Conn struct attributes
func (c *Conn) Flush() error {
	if c.FlushMock != nil {
		return c.FlushMock()
	}

	if len(c.queue) > 0 {
		for _, cmd := range c.queue {
			reply, err := c.do(cmd.commandName, cmd.args...)
			c.replies = append(c.replies, replyElement{reply: reply, err: err})
		}
		c.queue = []queueElement{}
	}

	return nil
}

// AddSubscriptionMessage register a response to be returned by the receive
// call.
func (c *Conn) AddSubscriptionMessage(msg interface{}) {
	resp := Response{}
	resp.Response = msg
	c.SubResponses = append(c.SubResponses, resp)
}

// Receive will process the queue created by the Send method, only one item
// of the queue is processed by Receive call. It will work as the Do method
func (c *Conn) Receive() (reply interface{}, err error) {
	if c.ReceiveWait {
		<-c.ReceiveNow
	}

	if len(c.queue) == 0 && len(c.replies) == 0 {
		if len(c.SubResponses) > 0 {
			reply, err = c.SubResponses[0].Response, c.SubResponses[0].Error
			c.SubResponses = c.SubResponses[1:]
			return
		}
		return nil, fmt.Errorf("no more items")
	}

	if err := c.Flush(); err != nil {
		return nil, err
	}

	reply, err = c.replies[0].reply, c.replies[0].err
	c.replies = c.replies[1:]
	return
}

// ReceiveWithTimeout is a helper function for Receive call to satisfy the
// ConnWithTimeout interface.
func (c *Conn) ReceiveWithTimeout(timeout time.Duration) (interface{}, error) {
	return c.Receive()
}

// Stats returns the number of times that a command was called in the current
// connection
func (c *Conn) Stats(cmd *Cmd) int {
	c.statsMut.RLock()
	defer c.statsMut.RUnlock()

	return c.stats[cmd.hash()]
}

// ExpectationsWereMet can guarantee that all commands that was set on unit tests
// called or call of unregistered command can be caught here too
func (c *Conn) ExpectationsWereMet() error {
	errMsg := ""
	for _, err := range c.Errors {
		errMsg = fmt.Sprintf("%s%s\n", errMsg, err.Error())
	}

	for _, cmd := range c.commands {
		if !cmd.Called {
			errMsg = fmt.Sprintf("%sCommand %s with arguments %#v expected but never called.\n", errMsg, cmd.Name, cmd.Args)
		}
	}

	if errMsg != "" {
		return fmt.Errorf("%s", errMsg)
	}

	return nil
}
