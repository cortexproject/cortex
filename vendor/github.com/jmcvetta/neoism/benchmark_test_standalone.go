// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

// +build !appengine

package neoism

import (
	"log"
	"testing"
)

func connectBench(b *testing.B) *Database {
	log.SetFlags(log.Ltime | log.Lshortfile)
	db, err := Connect("http://localhost:7474/db/data")
	if err != nil {
		b.Fatal(err)
	}
	return db
}
