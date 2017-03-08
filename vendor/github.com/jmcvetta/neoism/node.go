// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released under
// the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for details.
// Resist intellectual serfdom - the ownership of ideas is akin to slavery.

package neoism

import (
	"errors"
	"strconv"
	"strings"
)

// CreateNode creates a Node in the database.
func (db *Database) CreateNode(p Props) (*Node, error) {
	n := Node{}
	n.Db = db
	ne := NeoError{}
	resp, err := db.Session.Post(db.HrefNode, &p, &n, &ne)
	if err != nil {
		return &n, err
	}
	switch resp.Status() {
	case 201: // Success
		return &n, nil
	case 404:
		return nil, NotFound
	}
	return nil, ne
}

// Node fetches a Node from the database
func (db *Database) Node(id int) (*Node, error) {
	uri := join(db.HrefNode, strconv.Itoa(id))
	return db.getNodeByUri(uri)
}

// GetOrCreateNode creates a node if it doesn’t already exist.
func (db *Database) GetOrCreateNode(label, key string, p Props) (n *Node, created bool, err error) {
	/*
		valInterface, ok := p[key]
		if !ok {
			return nil, false, errors.New("Properties must contain key")
		}
		value, ok := valInterface.(string)
		if !ok {
			return nil, false, errors.New("Value of key must be a string")
		}
	*/
	value, ok := p[key]
	if !ok {
		return nil, false, errors.New("Properties must contain key")
	}
	n = &Node{}
	n.Db = db
	ne := NeoError{}
	uri := join(db.HrefNodeIndex, label) + "?uniqueness=get_or_create"
	type s struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
		Props Props       `json:"properties"`
	}
	payload := s{
		Key:   key,
		Value: value,
		Props: p,
	}
	resp, err := db.Session.Post(uri, &payload, &n, &ne)
	if err != nil {
		return nil, false, err
	}
	switch resp.Status() {
	case 200:
		return n, false, nil // Existing node
	case 201:
		return n, true, nil // Created node
	}
	return nil, false, ne // Error
}

// getNodeByUri fetches a Node from the database based on its URI.
func (db *Database) getNodeByUri(uri string) (*Node, error) {
	n := Node{}
	n.Db = db
	ne := NeoError{}
	resp, err := db.Session.Get(uri, nil, &n, &ne)
	if err != nil {
		return nil, err
	}
	status := resp.Status()
	switch {
	case status == 404:
		return &n, NotFound
	case status != 200 || n.HrefSelf == "":
		return nil, ne
	}
	return &n, nil
}

// A Node is a node, with optional properties, in a graph.
type Node struct {
	entity
	HrefOutgoingRels      string                 `json:"outgoing_relationships"`
	HrefTraverse          string                 `json:"traverse"`
	HrefAllTypedRels      string                 `json:"all_typed_relationships"`
	HrefOutgoing          string                 `json:"outgoing_typed_relationships"`
	HrefIncomingRels      string                 `json:"incoming_relationships"`
	HrefCreateRel         string                 `json:"create_relationship"`
	HrefPagedTraverse     string                 `json:"paged_traverse"`
	HrefAllRels           string                 `json:"all_relationships"`
	HrefIncomingTypedRels string                 `json:"incoming_typed_relationships"`
	HrefLabels            string                 `json:"labels"`
	Data                  map[string]interface{} `json:"data"`
	Extensions            map[string]interface{} `json:"extensions"`
}

// Id gets the ID number of this Node.
func (n *Node) Id() int {
	l := len(n.Db.HrefNode)
	s := n.HrefSelf[l:]
	s = strings.Trim(s, "/")
	id, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return id
}

// getRels makes an api call to the supplied uri and returns a map
// keying relationship IDs to Rel objects.
func (n *Node) getRels(uri string, types ...string) (Rels, error) {
	if types != nil {
		fragment := strings.Join(types, "&")
		parts := []string{uri, fragment}
		uri = strings.Join(parts, "/")
	}
	rels := Rels{}
	ne := NeoError{}
	resp, err := n.Db.Session.Get(uri, nil, &rels, &ne)
	if err != nil {
		return rels, err
	}
	if resp.Status() != 200 {
		return rels, ne
	}
	for _, rel := range rels {
		rel.Db = n.Db
	}
	return rels, nil // Success!
}

// Rels gets all Rels for this Node, optionally filtered by
// type, returning them as a map keyed on Rel ID.
func (n *Node) Relationships(types ...string) (Rels, error) {
	return n.getRels(n.HrefAllRels, types...)
}

// Incoming gets all incoming Rels for this Node.
func (n *Node) Incoming(types ...string) (Rels, error) {
	return n.getRels(n.HrefIncomingRels, types...)
}

// Outgoing gets all outgoing Rels for this Node.
func (n *Node) Outgoing(types ...string) (Rels, error) {
	return n.getRels(n.HrefOutgoingRels, types...)
}

// Relate creates a relationship of relType, with specified properties,
// from this Node to the node identified by destId.
func (n *Node) Relate(relType string, destId int, p Props) (*Relationship, error) {
	rel := Relationship{}
	rel.Db = n.Db
	srcUri := join(n.HrefSelf, "relationships")
	destUri := join(n.Db.HrefNode, strconv.Itoa(destId))
	content := map[string]interface{}{
		"to":   destUri,
		"type": relType,
	}
	if p != nil {
		content["data"] = &p
	}
	ne := NeoError{}
	resp, err := n.Db.Session.Post(srcUri, content, &rel, &ne)
	if err != nil {
		return &rel, err
	}
	if resp.Status() != 201 {
		return &rel, ne
	}
	return &rel, nil
}

// AddLabels adds one or more labels to a node.
func (n *Node) AddLabel(labels ...string) error {
	ne := NeoError{}
	resp, err := n.Db.Session.Post(n.HrefLabels, labels, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 404 {
		return NotFound
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success
}

// Labels lists labels for a node.
func (n *Node) Labels() ([]string, error) {
	res := []string{}
	ne := NeoError{}
	resp, err := n.Db.Session.Get(n.HrefLabels, nil, &res, &ne)
	if err != nil {
		return res, err
	}
	if resp.Status() == 404 {
		return res, NotFound
	}
	if resp.Status() != 200 {
		return res, ne
	}
	return res, nil // Success
}

// RemoveLabel removes a label from a node.
func (n *Node) RemoveLabel(label string) error {
	uri := join(n.HrefLabels, label)
	ne := NeoError{}
	resp, err := n.Db.Session.Delete(uri, nil, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 404 {
		return NotFound
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success
}

// SetLabels removes any labels currently on a node, and replaces them with the
// labels provided as argument.
func (n *Node) SetLabels(labels []string) error {
	ne := NeoError{}
	resp, err := n.Db.Session.Put(n.HrefLabels, labels, nil, &ne)
	if err != nil {
		return err
	}
	if resp.Status() == 404 {
		return NotFound
	}
	if resp.Status() != 204 {
		return ne
	}
	return nil // Success
}

// NodesByLabel gets all nodes with a given label.
func (db *Database) NodesByLabel(label string) ([]*Node, error) {
	uri := join(db.Url, "label", label, "nodes")
	res := []*Node{}
	ne := NeoError{}
	resp, err := db.Session.Get(uri, nil, &res, &ne)
	if err != nil {
		return res, err
	}
	if resp.Status() == 404 {
		return res, NotFound
	}
	if resp.Status() != 200 {
		return res, ne
	}
	for _, n := range res {
		n.Db = db
	}
	return res, nil // Success
}

// Labels lists all labels.
func (db *Database) Labels() ([]string, error) {
	uri := join(db.Url, "labels")
	labels := []string{}
	ne := NeoError{}
	resp, err := db.Session.Get(uri, nil, &labels, &ne)
	if err != nil {
		return labels, err
	}
	if resp.Status() != 200 {
		return labels, ne
	}
	return labels, nil
}
