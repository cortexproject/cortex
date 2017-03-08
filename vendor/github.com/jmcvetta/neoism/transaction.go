// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"encoding/json"
	"errors"
)

// A Tx is an in-progress database transaction.
type Tx struct {
	db         *Database
	hrefCommit string
	Location   string
	Errors     []TxError
	Expires    string // Cannot unmarshall into time.Time :(
}

type txRequest struct {
	Statements []*CypherQuery `json:"statements"`
}

type txResponse struct {
	Commit  string
	Results []struct {
		Columns []string
		Data    []struct {
			Row []*json.RawMessage
		}
		Stats *Stats
	}
	Transaction struct {
		Expires string
	}
	Errors []TxError
}

// unmarshal populates a slice of CypherQuery object with result data returned
// from the server.
func (tr *txResponse) unmarshal(qs []*CypherQuery) error {
	if len(tr.Results) != len(qs) {
		return errors.New("Result count does not match query count")
	}
	// NOTE: Beginning in 2.0.0-M05, the data format returned by transaction
	// endpoint diverged from the format returned by cypher batch.  At least
	// until final 2.0.0 release, we will work around this by munging the new
	// result format into the existing cypherResult struct.
	for i, res := range tr.Results {
		data := make([][]*json.RawMessage, len(res.Data))
		for n, d := range res.Data {
			data[n] = d.Row
		}
		q := qs[i]
		cr := cypherResult{
			Columns: res.Columns,
			Data:    data,
			Stats:   res.Stats,
		}
		q.cr = cr
		if q.Result != nil {
			err := q.Unmarshal(q.Result)
			if err != nil {
				return err
			}
		}
		q.stats = cr.Stats
	}
	return nil
}

// Begin opens a new transaction, executing zero or more cypher queries
// inside the transaction.
func (db *Database) Begin(qs []*CypherQuery) (*Tx, error) {
	payload := txRequest{Statements: qs}
	result := txResponse{}
	ne := NeoError{}
	resp, err := db.Session.Post(db.HrefTransaction, payload, &result, &ne)
	if err != nil {
		return nil, err
	}
	if resp.Status() != 201 {
		return nil, ne
	}
	t := Tx{
		db:         db,
		hrefCommit: result.Commit,
		Location:   resp.HttpResponse().Header.Get("Location"),
		Errors:     result.Errors,
		Expires:    result.Transaction.Expires,
	}
	if len(t.Errors) != 0 {
		return &t, TxQueryError
	}
	err = result.unmarshal(qs)
	if err != nil {
		return &t, err
	}
	return &t, err
}

// Commit commits an open transaction.
func (t *Tx) Commit() error {
	if len(t.Errors) > 0 {
		return TxQueryError
	}
	ne := NeoError{}
	resp, err := t.db.Session.Post(t.hrefCommit, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 200 {
		return ne
	}
	return nil // Success
}

// Query executes statements in an open transaction.
func (t *Tx) Query(qs []*CypherQuery) error {
	payload := txRequest{Statements: qs}
	result := txResponse{}
	ne := NeoError{}
	resp, err := t.db.Session.Post(t.Location, payload, &result, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 404 {
		return NotFound
	}
	if resp.Status() != 200 {
		return &ne
	}
	t.Expires = result.Transaction.Expires
	t.Errors = append(t.Errors, result.Errors...)
	if len(t.Errors) != 0 {
		return TxQueryError
	}
	err = result.unmarshal(qs)
	if err != nil {
		return err
	}
	return nil
}

// Rollback rolls back an open transaction.
func (t *Tx) Rollback() error {
	ne := NeoError{}
	resp, err := t.db.Session.Delete(t.Location, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 200 {
		return ne
	}
	return nil // Success
}
