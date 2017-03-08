// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package main

import (
	"fmt"
	"github.com/jmcvetta/neoism"
	"log"
)

func connect() *neoism.Database {
	db, err := neoism.Connect("http://localhost:7474/db/data")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func create(db *neoism.Database) {
	kirk, err := db.CreateNode(neoism.Props{"name": "Kirk", "shirt": "yellow"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(kirk.Properties()) // map[shirt:yellow name:Kirk] <nil>
	// Ignoring subsequent errors for brevity
	spock, _ := db.CreateNode(neoism.Props{"name": "Spock", "shirt": "blue"})
	mccoy, _ := db.CreateNode(neoism.Props{"name": "McCoy", "shirt": "blue"})
	r, _ := kirk.Relate("outranks", spock.Id(), nil) // No properties on this relationship
	start, _ := r.Start()
	fmt.Println(start.Properties()) // map[name:Kirk shirt:yellow] <nil>
	kirk.Relate("outranks", mccoy.Id(), nil)
	spock.Relate("outranks", mccoy.Id(), nil)
}

func transaction(db *neoism.Database) {
	qs := []*neoism.CypherQuery{
		&neoism.CypherQuery{
			Statement: `CREATE (n {name: "Scottie", shirt: "red"}) RETURN n`,
		},
		&neoism.CypherQuery{
			Statement: `START n=node(*), m=node(*)
				WHERE m.name = {name} AND n.shirt IN {colors}
				CREATE (n)-[r:outranks]->(m)
				RETURN n.name, type(r), m.name`,
			Parameters: neoism.Props{"name": "Scottie", "colors": []string{"yellow", "blue"}},
			Result: &[]struct {
				N   string `json:"n.name"` // `json` tag matches column name in query
				Rel string `json:"type(r)"`
				M   string `json:"m.name"`
			}{},
		},
	}
	tx, _ := db.Begin(qs)
	fmt.Println(qs[1].Result) // &[{Kirk outranks Scottie} {Spock outranks Scottie} {McCoy o...
	tx.Commit()
}

func cypher(db *neoism.Database) {
	cq := neoism.CypherQuery{
		Statement: `
			START n=node(*)
			MATCH (n)-[r:outranks]->(m)
			WHERE n.shirt = {color}
			RETURN n.name, type(r), m.name
			`,
		Parameters: neoism.Props{"color": "blue"},
		Result: &[]struct {
			N   string `json:"n.name"`
			Rel string `json:"type(r)"`
			M   string `json:"m.name"`
		}{},
	}
	// db.Session.Log = true
	db.Cypher(&cq)
	fmt.Println(cq.Result)
	// &[{Spock outranks McCoy} {Spock outranks Scottie} {McCoy outranks Scottie}]
}

func main() {
	db := connect()
	defer cleanup(db)
	create(db)
	transaction(db)
	cypher(db)
}

func cleanup(db *neoism.Database) {
	qs := []*neoism.CypherQuery{
		&neoism.CypherQuery{
			Statement: `START r=rel(*) DELETE r`,
		},
		&neoism.CypherQuery{
			Statement: `START n=node(*) DELETE n`,
		},
	}
	err := db.CypherBatch(qs)
	if err != nil {
		log.Fatal(err)
	}
}
