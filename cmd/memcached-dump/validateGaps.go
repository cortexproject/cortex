package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
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

		_, _, expectedIntervalMs, _, err := parseCacheKey(cache.Key)
		if err != nil {
			fmt.Println("Error parsing cache key: ", err)
			return
		}

		for _, e := range cache.Extents {
			for _, d := range e.Response.Data.Result {
				if len(d.Samples) > 1 {

					for i := 0; i < len(d.Samples)-2; i++ {
						actualIntervalMs := d.Samples[i+1].TimestampMs - d.Samples[i].TimestampMs

						if actualIntervalMs > expectedIntervalMs && time.Duration(actualIntervalMs)*time.Millisecond > minGap {
							hasGaps = true
						}
					}
				}
			}
		}

		totalQueries++
		if hasGaps {
			if len(cache.Extents) > 1 {
				fmt.Println("Oh no!  More than 1 extent!  Assume Real Gap.")
				totalGaps++
			} else {

				cached := cache.Extents[0].Response
				uncached, err := requery(cache, querierAddress)

				if err != nil {
					fmt.Println(err)
					return
				}

				if !reflect.DeepEqual(cached, uncached) {
					totalGaps++

					jsonCached, _ := json.Marshal(cached)
					jsonUncached, _ := json.Marshal(uncached)
					fmt.Println(string(jsonCached))
					fmt.Println(string(jsonUncached))
				} else {
					fakeGaps++
				}
			}

			fmt.Printf("Fake/Real/Total: %d/%d/%d (%f) \n", fakeGaps, totalGaps, totalQueries, float64(totalGaps)/float64(totalQueries))
		}
	}
}

func requery(cache *queryrange.CachedResponse, address string) (*queryrange.APIResponse, error) {
	userID, query, step, day, err := parseCacheKey(cache.Key)
	if err != nil {
		return nil, err
	}

	u := url.URL{}

	u.Scheme = "http"
	u.Host = address
	u.Path = "/api/prom/api/v1/query_range"

	q := u.Query()
	q.Set("query", query)
	q.Set("start", fmt.Sprint(day*secondsPerDay))
	q.Set("end", fmt.Sprint((day+1)*secondsPerDay-1)) // -1 : don't grab the first sample of the next day
	q.Set("step", fmt.Sprint(step/1000))

	u.RawQuery = q.Encode()

	fmt.Println(query)

	client := &http.Client{}
	req, err := http.NewRequest("GET", u.String(), nil)
	req.Header.Add("X-Scope-OrgID", userID)

	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	jsonBytes, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var value queryrange.APIResponse

	err = json.Unmarshal(jsonBytes, &value)

	if err != nil {
		return nil, err
	}
	return &value, nil
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
