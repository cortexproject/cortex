// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"net/url"
)

func (db *Database) createIndex(href, name, idxType, provider string) (*index, error) {
	idx := new(index)
	idx.db = db
	idx.Name = name
	type c struct {
		Type     string `json:"type,omitempty"`
		Provider string `json:"provider,omitempty"`
	}
	type p struct {
		Name   string `json:"name"`
		Config c      `json:"config,omitempty"`
	}
	payload := p{
		Name: name,
	}
	if idxType != "" || provider != "" {
		config := c{
			Type:     idxType,
			Provider: provider,
		}
		payload.Config = config
	}
	res := indexResponse{}
	ne := NeoError{}
	resp, err := db.Session.Post(href, &payload, &res, &ne)
	if err != nil {
		return nil, err
	}
	if resp.Status() != 201 {
		resp.Unmarshal(&ne)
		return nil, ne
	}
	idx.populate(&res)
	idx.HrefIndex = href
	return idx, nil
}

func (db *Database) indexes(href string) ([]*index, error) {
	res := map[string]indexResponse{}
	nis := []*index{}
	ne := NeoError{}
	resp, err := db.Session.Get(href, nil, &res, &ne)
	if err != nil {
		return nis, err
	}
	if resp.Status() != 200 {
		return nis, ne
	}
	for name, r := range res {
		n := index{}
		n.db = db
		n.Name = name
		n.populate(&r)
		nis = append(nis, &n)
	}
	return nis, nil
}

func (db *Database) index(href, name string) (*index, error) {
	idx := new(index)
	idx.db = db
	idx.Name = name
	idx.HrefIndex = href
	baseUri := href
	rawurl := join(baseUri, name)
	u, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return idx, err
	}
	ne := NeoError{}
	resp, err := db.Session.Get(u.String(), nil, nil, &ne)
	if err != nil {
		return nil, err
	}
	switch resp.Status() {
	// Success!
	case 200:
	case 404:
		return nil, NotFound
	default:
		return idx, ne
	}
	return idx, nil
}

type index struct {
	db            *Database
	Name          string
	Provider      string
	IndexType     string
	CaseSensitive bool
	HrefIndex     string
}

func (idx *index) populate(res *indexResponse) {
	idx.Provider = res.Provider
	idx.IndexType = res.IndexType
	if res.LowerCase == "true" {
		idx.CaseSensitive = false
	} else {
		idx.CaseSensitive = true
	}
}

type indexResponse struct {
	HrefTemplate string `json:"template"`
	Provider     string `json:"provider"`      // Not always populated by server
	IndexType    string `json:"type"`          // Not always populated by server
	LowerCase    string `json:"to_lower_case"` // Not always populated by server
}

// uri returns the URI for this Index.
func (idx *index) uri() (string, error) {
	s := join(idx.HrefIndex, idx.Name)
	u, err := url.ParseRequestURI(s)
	return u.String(), err
}

// Delete removes a index from the database.
func (idx *index) Delete() error {
	uri, err := idx.uri()
	if err != nil {
		return err
	}
	ne := NeoError{}
	resp, err := idx.db.Session.Delete(uri, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success!
}

// Add associates a Node with the given key/value pair in the given index.
func (idx *index) add(e entity, key string, value interface{}) error {
	uri, err := idx.uri()
	if err != nil {
		return err
	}
	type s struct {
		Uri   string      `json:"uri"`
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}
	payload := s{
		Uri:   e.HrefSelf,
		Key:   key,
		Value: value,
	}
	ne := NeoError{}
	resp, err := idx.db.Session.Post(uri, &payload, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 201 {
		return ne
	}
	return nil // Success!
}

func (idx *index) remove(e entity, id, key, value string) error {
	uri, err := idx.uri()
	if err != nil {
		return err
	}
	// Since join() ignores fragments that are empty strings, joining an empty
	// value with a non-empty key produces a valid URL.  But joining a non-empty
	// value with an empty key would produce an invalid URL wherein they value is
	// conflated with the key.
	if key != "" {
		uri = join(uri, key, value)
	}
	uri = join(uri, id)
	ne := NeoError{}
	resp, err := idx.db.Session.Delete(uri, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success!
}
