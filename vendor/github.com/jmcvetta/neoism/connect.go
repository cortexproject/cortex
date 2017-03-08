// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

// +build !appengine

package neoism

import (
	"net/http"
	"net/url"
	"strings"

	"gopkg.in/jmcvetta/napping.v3"
)

// Connect setups parameters for the Neo4j server
// and calls ConnectWithRetry()
func Connect(uri string) (*Database, error) {
	h := http.Header{}
	h.Add("User-Agent", "neoism")
	db := &Database{
		Session: &napping.Session{
			Header: &h,
		},
	}

	// trailing slash is important, check if it's not there and add it
	if !strings.HasSuffix(uri, "/") {
		uri += "/"
	}
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if parsedURL.User != nil {
		db.Session.Userinfo = parsedURL.User
	}
	return connectWithRetry(db, parsedURL, 0)
}
