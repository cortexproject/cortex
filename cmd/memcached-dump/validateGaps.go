package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

const secondsPerDay = 86400

func buildValidateGaps(minGap time.Duration, querierAddress string) processFunc {
	totalQueries := 0
	totalGaps := 0
	fakeGaps := 0

	return func(cache *queryrange.CachedResponse, b []byte) {
		hasGaps := false

		userId, _, expectedIntervalMs, _, err := parseCacheKey(cache.Key)
		if err != nil {
			fmt.Println("Error parsing cache key: ", err)
			return
		}

		for _, e := range cache.Extents {
			if e.Response.Status == "error" {
				return
			}

			for _, d := range e.Response.Data.Result {
				for i := 0; i < len(d.Samples)-1; i++ {
					actualIntervalMs := d.Samples[i+1].TimestampMs - d.Samples[i].TimestampMs

					if actualIntervalMs > expectedIntervalMs && time.Duration(actualIntervalMs)*time.Millisecond > minGap {
						hasGaps = true
					}
				}
			}

			totalQueries++
			if hasGaps {
				cached := e.Response
				uncached, url, err := requery(cache, e.Start/1000, e.End/1000, querierAddress)

				if err != nil {
					fmt.Println(err)
					return
				}

				jsonCached, _ := json.Marshal(cached)
				jsonUncached, _ := json.Marshal(uncached)

				if string(jsonCached) == string(jsonUncached) {
					fakeGaps++
				} else {
					totalGaps++

					fmt.Println("Gap Found: ")
					fmt.Println("User        : ", userId)
					fmt.Println("Trace       : ", e.TraceId)
					fmt.Println("requery url : ", url)
					fmt.Println("cached: ")
					fmt.Println(string(jsonCached))
					fmt.Println("uncached: ")
					fmt.Println(string(jsonUncached))
				}

				fmt.Printf("Fake/Real/Total: %d/%d/%d (%f) \n", fakeGaps, totalGaps, totalQueries, float64(totalGaps)/float64(totalQueries))
			}
		}
	}
}

func requery(cache *queryrange.CachedResponse, startSeconds int64, endSeconds int64, address string) (*queryrange.APIResponse, string, error) {
	userID, query, step, _, err := parseCacheKey(cache.Key)
	if err != nil {
		return nil, "", err
	}

	u := url.URL{}

	u.Scheme = "http"
	u.Host = address
	u.Path = "/api/prom/api/v1/query_range"

	q := u.Query()
	q.Set("query", query)
	q.Set("start", fmt.Sprint(startSeconds))
	q.Set("end", fmt.Sprint(endSeconds))
	q.Set("step", fmt.Sprint(step/1000))

	u.RawQuery = q.Encode()

	client := &http.Client{}
	req, err := http.NewRequest("GET", u.String(), nil)
	req.Header.Add("X-Scope-OrgID", userID)

	resp, err := client.Do(req)

	if err != nil {
		return nil, "", err
	}

	jsonBytes, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, "", err
	}

	var value queryrange.APIResponse

	err = json.Unmarshal(jsonBytes, &value)

	if err != nil {
		return nil, "", err
	}
	return &value, u.String(), nil
}

func parseCacheKey(key string) (string, string, int64, int64, error) {
	// build a query string from the cache key
	parts := strings.Split(key, ":")

	if len(parts) < 4 {
		return "", "", 0, 0, fmt.Errorf("unable to parse key %s", key)
	}

	userID := parts[0]
	query := strings.Join(parts[1:len(parts)-2], ":")
	stepMs, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("unable to parse step %s", parts[len(parts)-2])
	}
	day, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("unable to parse day %s", parts[len(parts)-1])
	}

	return userID, query, stepMs, day, nil
}
