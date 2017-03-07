// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

// +build appengine

package neoism

import "appengine/aetest"

func dbConnect(url string) (*Database, error) {
	gaeContext, err := aetest.NewContext(nil)
	if err != nil {
		return nil, err
	}

	return Connect(url, gaeContext)
}
