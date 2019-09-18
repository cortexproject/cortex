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

	return func(cache *queryrange.CachedResponse, b []byte) {
		hasGaps := false

		for _, e := range cache.Extents {
			for _, d := range e.Response.Data.Result {
				if len(d.Samples) > 1 {

					expectedIntervalMs := d.Samples[1].TimestampMs - d.Samples[0].TimestampMs
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

			err := requery(cache, querierAddress)

			if err != nil {
				fmt.Println(err)
				return
			}

			totalGaps++
			fmt.Printf("Gaps/Total: %d/%d\n", totalGaps, totalQueries)
			//fmt.Println(string(b))
		}
	}
}

func requery(cache *queryrange.CachedResponse, address string) error {
	// build a query string from the cache key
	parts := strings.Split(cache.Key, ":")

	if len(parts) != 4 {
		return fmt.Errorf("unable to parse key %s", cache.Key)
	}

	userID := parts[0]
	query := parts[1]
	step := parts[2]

	day, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse day %s", parts[3])
	}

	u := url.URL{}

	u.Scheme = "http"
	u.Host = address
	u.Path = "/api/prom/api/v1/query_range"

	q := u.Query()
	q.Set("query", query)
	q.Set("start", fmt.Sprint(day*secondsPerDay))
	q.Set("end", fmt.Sprint((day+1)*secondsPerDay))
	q.Set("step", step)

	u.RawQuery = q.Encode()

	fmt.Println(query)

	client := &http.Client{}
	req, err := http.NewRequest("GET", u.String(), nil)
	req.Header.Add("X-Scope-OrgID", userID)

	resp, err := client.Do(req)

	if err != nil {
		return err
	}

	jsonBytes, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	var value map[string]interface{}

	err = json.Unmarshal(jsonBytes, &value)

	if err != nil {
		return err
	}

	fmt.Println(value)

	return nil
}
