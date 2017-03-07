// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released
// under the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for
// details.  Resist intellectual serfdom - the ownership of ideas is akin to
// slavery.

// Example demonstrating use of package napping, with HTTP Basic
// authentictation over HTTPS, to retrieve a Github auth token.
package main

/*

NOTE: This example may only work on *nix systems due to gopass requirements.

*/

import (
	"fmt"
	"github.com/howeyc/gopass"
	"github.com/kr/pretty"
	"gopkg.in/jmcvetta/napping.v3"
	"log"
	"net/url"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func main() {
	//
	// Prompt user for Github username/password
	//
	var username string
	fmt.Printf("Github username: ")
	_, err := fmt.Scanf("%s", &username)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("github.com/howeyc/gopass")
	passwd, err := gopass.GetPasswd()
	if err != nil {
		log.Fatal(err)
	}
	//
	// Compose request
	//
	// http://developer.github.com/v3/oauth/#create-a-new-authorization
	//
	payload := struct {
		Scopes []string `json:"scopes"`
		Note   string   `json:"note"`
	}{
		Scopes: []string{"public_repo"},
		Note:   "testing Go napping" + time.Now().String(),
	}
	//
	// Struct to hold response data
	//
	res := struct {
		Id        int
		Url       string
		Scopes    []string
		Token     string
		App       map[string]string
		Note      string
		NoteUrl   string `json:"note_url"`
		UpdatedAt string `json:"updated_at"`
		CreatedAt string `json:"created_at"`
	}{}
	//
	// Struct to hold error response
	//
	e := struct {
		Message string
		Errors  []struct {
			Resource string
			Field    string
			Code     string
		}
	}{}
	//
	// Setup HTTP Basic auth for this session (ONLY use this with SSL).  Auth
	// can also be configured on a per-request basis when using Send().
	//
	s := napping.Session{
		Userinfo: url.UserPassword(username, string(passwd)),
	}
	url := "https://api.github.com/authorizations"
	//
	// Send request to server
	//
	resp, err := s.Post(url, &payload, &res, &e)
	if err != nil {
		log.Fatal(err)
	}
	//
	// Process response
	//
	println("")
	if resp.Status() == 201 {
		fmt.Printf("Github auth token: %s\n\n", res.Token)
	} else {
		fmt.Println("Bad response status from Github server")
		fmt.Printf("\t Status:  %v\n", resp.Status())
		fmt.Printf("\t Message: %v\n", e.Message)
		fmt.Printf("\t Errors: %v\n", e.Message)
		pretty.Println(e.Errors)
	}
	println("")
}
