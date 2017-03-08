// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"errors"
	"log"
	"net/url"
	"strconv"

	"gopkg.in/jmcvetta/napping.v3"
)

// A Database is a REST client connected to a Neo4j database.
type Database struct {
	Session         *napping.Session
	Url             string      `json:"-"` // Root URL for REST API
	HrefNode        string      `json:"node"`
	HrefRefNode     string      `json:"reference_node"`
	HrefNodeIndex   string      `json:"node_index"`
	HrefRelIndex    string      `json:"relationship_index"`
	HrefExtInfo     string      `json:"extensions_info"`
	HrefRelTypes    string      `json:"relationship_types"`
	HrefBatch       string      `json:"batch"`
	HrefCypher      string      `json:"cypher"`
	HrefTransaction string      `json:"transaction"`
	Version         string      `json:"neo4j_version"`
	Extensions      interface{} `json:"extensions"`
}

// connectWithRetry tries to establish a connection to the Neo4j server.
// If the ping successes but doesn't return version,
// it retries using Path "/db/data/" with a max number of retries of 3.
func connectWithRetry(db *Database, parsedUrl *url.URL, retries int) (*Database, error) {
	if retries > 3 {
		return nil, errors.New("Failed too many times")
	}
	db.Url = parsedUrl.String()
	//		Url:    db.Url,
	//		Method: "GET",
	//		Result: &db,
	//		Error:  &e,
	resp, err := db.Session.Get(db.Url, nil, &db, nil)
	if err != nil {
		return nil, err
	}
	if resp.Status() != 200 {
		log.Println("Status " + strconv.Itoa(resp.Status()) + " trying to connect to " + db.Url)
		return nil, InvalidDatabase
	}
	if db.Version == "" {
		parsedUrl.Path = "/db/data/"
		return connectWithRetry(db, parsedUrl, retries+1)
	}
	return db, nil
}

// PropertyKeys lists all property keys ever used in the database. This
// includes and property keys you have used, but deleted.  There is
// currently no way to tell which ones are in use and which ones are not,
// short of walking the entire set of properties in the database.
func PropertyKeys(db *Database) ([]string, error) {
	propertyKeys := []string{}
	ne := NeoError{}

	uri := db.Url + "propertykeys"
	resp, err := db.Session.Get(uri, nil, &propertyKeys, &ne)
	if err != nil {
		return propertyKeys, err
	}
	if resp.Status() != 200 {
		return propertyKeys, ne
	}
	return propertyKeys, err
}

// A Props is a set of key/value properties.
type Props map[string]interface{}
