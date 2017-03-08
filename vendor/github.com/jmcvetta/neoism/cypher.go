// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"encoding/json"
	"errors"
)

// A CypherQuery is a statement in the Cypher query language, with optional
// parameters and result.  If Result value is supplied, result data will be
// unmarshalled into it when the query is executed. Result must be a pointer
// to a slice of structs - e.g. &[]someStruct{}.
type CypherQuery struct {
	Statement    string                 `json:"statement"`
	Parameters   map[string]interface{} `json:"parameters"`
	Result       interface{}            `json:"-"`
	cr           cypherResult
	IncludeStats bool `json:"includeStats"`
	stats        *Stats
}

// Columns returns the names, in order, of the columns returned for this query.
// Empty if query has not been executed.
func (cq *CypherQuery) Columns() []string {
	return cq.cr.Columns
}

// Unmarshal decodes result data into v, which must be a pointer to a slice of
// structs - e.g. &[]someStruct{}.  Struct fields are matched up with fields
// returned by the cypher query using the `json:"fieldName"` tag.
func (cq *CypherQuery) Unmarshal(v interface{}) error {
	// We do a round-trip thru the JSON marshaller.  A fairly simple way to
	// do type-safe unmarshalling, but perhaps not the most efficient solution.
	rs := make([]map[string]*json.RawMessage, len(cq.cr.Data))
	for rowNum, row := range cq.cr.Data {
		m := map[string]*json.RawMessage{}
		for colNum, col := range row {
			name := cq.cr.Columns[colNum]
			m[name] = col
		}
		rs[rowNum] = m
	}
	b, err := json.Marshal(rs)
	if err != nil {
		logPretty(err)
		return err
	}
	return json.Unmarshal(b, v)
}

func (cq *CypherQuery) Stats() (*Stats, error) {
	if cq.stats == nil {
		return nil, errors.New("stats were not requested at query time")
	}
	return cq.stats, nil
}

type cypherRequest struct {
	Query      string                 `json:"query"`
	Parameters map[string]interface{} `json:"params"`
}

type Stats struct {
	ConstraintsAdded     int  `json:"constraints_added"`
	ConstraintsRemoved   int  `json:"constraints_removed"`
	ContainsUpdates      bool `json:"contains_updates"`
	IndexesAdded         int  `json:"indexes_added"`
	IndexesRemoved       int  `json:"indexes_removed"`
	LabelsAdded          int  `json:"labels_added"`
	LabelsRemoved        int  `json:"labels_removed"`
	NodesCreated         int  `json:"nodes_created"`
	NodesDeleted         int  `json:"nodes_deleted"`
	PropertiesSet        int  `json:"properties_set"`
	RelationshipDeleted  int  `json:"relationship_deleted"`
	RelationshipsCreated int  `json:"relationships_created"`
}

type cypherResult struct {
	Columns []string
	Data    [][]*json.RawMessage
	Stats   *Stats
}

// Cypher executes a db query written in the Cypher language.  Data returned
// from the db is used to populate `result`, which should be a pointer to a
// slice of structs.  TODO:  Or a pointer to a two-dimensional array of structs?
func (db *Database) Cypher(q *CypherQuery) error {
	result := cypherResult{}
	payload := cypherRequest{
		Query:      q.Statement,
		Parameters: q.Parameters,
	}
	ne := NeoError{}
	url := db.HrefCypher
	if q.IncludeStats {
		url = db.HrefCypher + "?includeStats=true"
	}
	// Method: "POST"
	// Data:   &cReq
	// Result: &cRes
	// Error:  ne
	// }
	resp, err := db.Session.Post(url, &payload, &result, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 200 {
		return ne
	}
	q.cr = result
	if q.Result != nil {
		q.Unmarshal(q.Result)
	}
	q.stats = q.cr.Stats
	return nil
}

type batchCypherQuery struct {
	Method string        `json:"method"`
	To     string        `json:"to"`
	Id     int           `json:"id"`
	Body   cypherRequest `json:"body"`
}

type batchCypherResponse struct {
	Id       int
	Location string
	Body     cypherResult
}

// CypherBatch executes a set of cypher queries as a batch.  When using the
// {[JOB ID]} special syntax to inject URIs from created resources into JSON
// strings in subsequent job descriptions, CypherQuery's batch id will be its
// index in the slice.
func (db *Database) CypherBatch(qs []*CypherQuery) error {
	payload := make([]batchCypherQuery, len(qs))
	for i, q := range qs {
		payload[i] = batchCypherQuery{
			Method: "POST",
			To:     "/cypher",
			Id:     i,
			Body: cypherRequest{
				Query:      q.Statement,
				Parameters: q.Parameters,
			},
		}
		if q.IncludeStats {
			payload[i].To = "/cypher?includeStats=true"
		}
	}
	res := []batchCypherResponse{}
	ne := NeoError{}
	url := db.HrefBatch
	resp, err := db.Session.Post(url, payload, &res, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 200 {
		return ne
	}
	if len(res) != len(qs) {
		return errors.New("Result count does not match query count")
	}
	for i, s := range qs {
		s.cr = res[i].Body
		if s.Result != nil {
			err := s.Unmarshal(s.Result)
			if err != nil {
				return err
			}
		}
		s.stats = s.cr.Stats
	}
	return nil
}
