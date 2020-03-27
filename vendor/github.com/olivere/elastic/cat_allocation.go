// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/olivere/elastic/uritemplates"
)

// CatAllocationService provides a snapshot of how many shards are allocated
// to each data node and how much disk space they are using.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/cat-allocation.html
// for details.
type CatAllocationService struct {
	client        *Client
	pretty        bool
	bytes         string // b, k, m, or g
	local         *bool
	masterTimeout string
	nodes         []string
	columns       []string
	sort          []string // list of columns for sort order
}

// NewCatAllocationService creates a new CatAllocationService.
func NewCatAllocationService(client *Client) *CatAllocationService {
	return &CatAllocationService{
		client: client,
	}
}

// NodeID specifies one or more node IDs to for information should be returned.
func (s *CatAllocationService) NodeID(nodes ...string) *CatAllocationService {
	s.nodes = nodes
	return s
}

// Bytes represents the unit in which to display byte values.
// Valid values are: "b", "k", "m", or "g".
func (s *CatAllocationService) Bytes(bytes string) *CatAllocationService {
	s.bytes = bytes
	return s
}

// Local indicates to return local information, i.e. do not retrieve
// the state from master node (default: false).
func (s *CatAllocationService) Local(local bool) *CatAllocationService {
	s.local = &local
	return s
}

// MasterTimeout is the explicit operation timeout for connection to master node.
func (s *CatAllocationService) MasterTimeout(masterTimeout string) *CatAllocationService {
	s.masterTimeout = masterTimeout
	return s
}

// Columns to return in the response.
// To get a list of all possible columns to return, run the following command
// in your terminal:
//
// Example:
//   curl 'http://localhost:9200/_cat/aliases?help'
//
// You can use Columns("*") to return all possible columns. That might take
// a little longer than the default set of columns.
func (s *CatAllocationService) Columns(columns ...string) *CatAllocationService {
	s.columns = columns
	return s
}

// Sort is a list of fields to sort by.
func (s *CatAllocationService) Sort(fields ...string) *CatAllocationService {
	s.sort = fields
	return s
}

// Pretty indicates that the JSON response be indented and human readable.
func (s *CatAllocationService) Pretty(pretty bool) *CatAllocationService {
	s.pretty = pretty
	return s
}

// buildURL builds the URL for the operation.
func (s *CatAllocationService) buildURL() (string, url.Values, error) {
	// Build URL
	var (
		path string
		err  error
	)

	if len(s.nodes) > 0 {
		path, err = uritemplates.Expand("/_cat/allocation/{node_id}", map[string]string{
			"node_id": strings.Join(s.nodes, ","),
		})
	} else {
		path = "/_cat/allocation"
	}
	if err != nil {
		return "", url.Values{}, err
	}

	// Add query string parameters
	params := url.Values{
		"format": []string{"json"}, // always returns as JSON
	}
	if s.pretty {
		params.Set("pretty", "true")
	}
	if s.bytes != "" {
		params.Set("bytes", s.bytes)
	}
	if v := s.local; v != nil {
		params.Set("local", fmt.Sprint(*v))
	}
	if s.masterTimeout != "" {
		params.Set("master_timeout", s.masterTimeout)
	}
	if len(s.sort) > 0 {
		params.Set("s", strings.Join(s.sort, ","))
	}
	if len(s.columns) > 0 {
		params.Set("h", strings.Join(s.columns, ","))
	}
	return path, params, nil
}

// Do executes the operation.
func (s *CatAllocationService) Do(ctx context.Context) (CatAllocationResponse, error) {
	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := s.client.PerformRequest(ctx, PerformRequestOptions{
		Method: "GET",
		Path:   path,
		Params: params,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	var ret CatAllocationResponse
	if err := s.client.decoder.Decode(res.Body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Result of a get request.

// CatAllocationResponse is the outcome of CatAllocationService.Do.
type CatAllocationResponse []CatAllocationResponseRow

// CatAllocationResponseRow is a single row in a CatAllocationResponse.
// Notice that not all of these fields might be filled; that depends
// on the number of columns chose in the request (see CatAllocationService.Columns).
type CatAllocationResponseRow struct {
	// Shards represents the number of shards on a node.
	Shards int `json:"shards,string"`
	// DiskIndices represents the disk used by ES indices, e.g. "46.1kb".
	DiskIndices string `json:"disk.indices"`
	// DiskUsed represents the disk used (total, not just ES), e.g. "34.5gb"
	DiskUsed string `json:"disk.used"`
	// DiskAvail represents the disk available, e.g. "53.2gb".
	DiskAvail string `json:"disk.avail"`
	// DiskTotal represents the total capacity of all volumes, e.g. "87.7gb".
	DiskTotal string `json:"disk.total"`
	// DiskPercent represents the percent of disk used, e.g. 39.
	DiskPercent int `json:"disk.percent,string"`
	// Host represents the hostname of the node.
	Host string `json:"host"`
	// IP represents the IP address of the node.
	IP string `json:"ip"`
	// Node represents the node ID.
	Node string `json:"node"`
}
