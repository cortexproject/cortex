package merger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV1Silences(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2Silences. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{
		[]byte(`{"status":"success","data":[` +
			`{` +
			`"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5",` +
			`"matchers":[` +
			`{` +
			`"name":"instance",` +
			`"value":"prometheus-one",` +
			`"isRegex":false,` +
			`"isEqual":true` +
			`}` +
			`],` +
			`"startsAt":"2021-04-28T17:31:01.725956017Z",` +
			`"endsAt":"2021-04-28T20:31:01.722829007+02:00",` +
			`"updatedAt":"2021-04-28T17:31:01.725956017Z",` +
			`"createdBy":"",` +
			`"comment":"Silence Comment #1",` +
			`"status":{"state":"active"}` +
			`},` +
			`{` +
			`"id":"17526003-c745-4464-a355-4f06de26a236",` +
			`"matchers":[` +
			`{` +
			`"name":"instance",` +
			`"value":"prometheus-one",` +
			`"isRegex":false,` +
			`"isEqual":true` +
			`}` +
			`],` +
			`"startsAt":"2021-04-28T17:31:01.731140275Z",` +
			`"endsAt":"2021-04-28T18:31:01.727579131Z",` +
			`"updatedAt":"2021-04-28T17:31:01.731140275Z",` +
			`"createdBy":"",` +
			`"comment":"Silence Comment #2",` +
			`"status":{"state":"active"}},` +
			`{` +
			`"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b",` +
			`"matchers":[` +
			`{` +
			`"name":"instance",` +
			`"value":"prometheus-one",` +
			`"isRegex":false,` +
			`"isEqual":true` +
			`}` +
			`],` +
			`"startsAt":"2021-04-28T17:31:01.73572697Z",` +
			`"endsAt":"2021-04-28T18:31:01.732873879Z",` +
			`"updatedAt":"2021-04-28T17:31:01.73572697Z",` +
			`"createdBy":"",` +
			`"comment":"Silence Comment #3",` +
			`"status":{"state":"active"}}` +
			`]}`),
		[]byte(`{"status":"success","data":[]}`),
	}

	// Note that our implementation for v1 uses v2 code internally. This means the JSON fields
	// come out in a slightly different order, and the timestamps lave less digits.
	expected := []byte(`{"status":"success","data":[` +
		`{` +
		`"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5",` +
		`"status":{"state":"active"},` +
		`"updatedAt":"2021-04-28T17:31:01.725Z",` +
		`"comment":"Silence Comment #1",` +
		`"createdBy":"",` +
		`"endsAt":"2021-04-28T20:31:01.722+02:00",` +
		`"matchers":[` +
		`{` +
		`"isEqual":true,` +
		`"isRegex":false,` +
		`"name":"instance",` +
		`"value":"prometheus-one"` +
		`}` +
		`],` +
		`"startsAt":"2021-04-28T17:31:01.725Z"` +
		`},` +
		`{` +
		`"id":"17526003-c745-4464-a355-4f06de26a236",` +
		`"status":{"state":"active"},` +
		`"updatedAt":"2021-04-28T17:31:01.731Z",` +
		`"comment":"Silence Comment #2",` +
		`"createdBy":"",` +
		`"endsAt":"2021-04-28T18:31:01.727Z",` +
		`"matchers":[` +
		`{` +
		`"isEqual":true,` +
		`"isRegex":false,` +
		`"name":"instance",` +
		`"value":"prometheus-one"` +
		`}` +
		`],` +
		`"startsAt":"2021-04-28T17:31:01.731Z"` +
		`},` +
		`{` +
		`"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b",` +
		`"status":{"state":"active"},` +
		`"updatedAt":"2021-04-28T17:31:01.735Z",` +
		`"comment":"Silence Comment #3",` +
		`"createdBy":"",` +
		`"endsAt":"2021-04-28T18:31:01.732Z",` +
		`"matchers":[` +
		`{` +
		`"isEqual":true,` +
		`"isRegex":false,` +
		`"name":"instance",` +
		`"value":"prometheus-one"` +
		`}` +
		`],` +
		`"startsAt":"2021-04-28T17:31:01.735Z"` +
		`}` +
		`]}`)

	out, err := V1Silences{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}
