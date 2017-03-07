// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

// +build appengine

package neoism

import (
	"net/http"
	"net/url"

	"appengine"
	"appengine/urlfetch"

	"gopkg.in/jmcvetta/napping.v3"
)

// Modified version of Connect that support Google App Engine.
// Connect setups parameters for the Neo4j server
// and calls ConnectWithRetry()
func Connect(uri string, gaeContext appengine.Context) (*Database, error) {
	h := http.Header{}
	h.Add("User-Agent", "neoism")
	db := &Database{
		Session: &napping.Session{
			Header: &h,
			Client: urlfetch.Client(gaeContext),
		},
	}
	parsedUrl, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if parsedUrl.User != nil {
		db.Session.Userinfo = parsedUrl.User
	}
	return connectWithRetry(db, parsedUrl, 0)
}
