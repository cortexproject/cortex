// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

/*

Package neoism is a client library providing access to the Neo4j graph database
via its REST API.


Example Usage:

	package main

	import (
		"fmt"
		"github.com/jmcvetta/neoism"
	)

	func main() {
		// No error handling in this example - bad, bad, bad!
		//
		// Connect to the Neo4j server
		//
		db, _ := neoism.Connect("http://localhost:7474/db/data")
		kirk := "Captain Kirk"
		mccoy := "Dr McCoy"
		//
		// Create a node
		//
		n0, _ := db.CreateNode(neoism.Props{"name": kirk})
		defer n0.Delete()  // Deferred clean up
		n0.AddLabel("Person") // Add a label
		//
		// Create a node with a Cypher query
		//
		res0 := []struct {
			N neoism.Node // Column "n" gets automagically unmarshalled into field N
		}{}
		cq0 := neoism.CypherQuery{
			Statement: "CREATE (n:Person {name: {name}}) RETURN n",
			// Use parameters instead of constructing a query string
			Parameters: neoism.Props{"name": mccoy},
			Result:     &res0,
		}
		db.Cypher(&cq0)
		n1 := res0[0].N // Only one row of data returned
		n1.Db = db // Must manually set Db with objects returned from Cypher query
		//
		// Create a relationship
		//
		n1.Relate("reports to", n0.Id(), neoism.Props{}) // Empty Props{} is okay
		//
		// Issue a query
		//
		res1 := []struct {
			A   string `json:"a.name"` // `json` tag matches column name in query
			Rel string `json:"type(r)"`
			B   string `json:"b.name"`
		}{}
		cq1 := neoism.CypherQuery{
			// Use backticks for long statements - Cypher is whitespace indifferent
			Statement: `
				MATCH (a:Person)-[r]->(b)
				WHERE a.name = {name}
				RETURN a.name, type(r), b.name
			`,
			Parameters: neoism.Props{"name": mccoy},
			Result:     &res1,
		}
		db.Cypher(&cq1)
		r := res1[0]
		fmt.Println(r.A, r.Rel, r.B)
		//
		// Clean up using a transaction
		//
		qs := []*neoism.CypherQuery{
			&neoism.CypherQuery{
				Statement: `
					MATCH (n:Person)-[r]->()
					WHERE n.name = {name}
					DELETE r
				`,
				Parameters: neoism.Props{"name": mccoy},
			},
			&neoism.CypherQuery{
				Statement: `
					MATCH (n:Person)
					WHERE n.name = {name}
					DELETE n
				`,
				Parameters: neoism.Props{"name": mccoy},
			},
		}
		tx, _ := db.Begin(qs)
		tx.Commit()
	}
*/
package neoism

// Imports required for tests - so they work with "go get".
import (
	_ "github.com/stretchr/testify/assert"
	_ "github.com/jmcvetta/randutil"
)
