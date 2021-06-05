package merger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV1SilenceID_ReturnsNewestSilence(t *testing.T) {
	in := [][]byte{
		[]byte(`{"status":"success","data":{` +
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
			`"updatedAt":"2021-04-28T17:32:01.725956017Z",` +
			`"createdBy":"",` +
			`"comment":"The newer silence",` +
			`"status":{"state":"active"}` +
			`}}`),
		[]byte(`{"status":"success","data":{` +
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
			`}}`),
	}

	expected := []byte(`{"status":"success","data":{` +
		`"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5",` +
		`"status":{"state":"active"},` +
		`"updatedAt":"2021-04-28T17:32:01.725Z",` +
		`"comment":"The newer silence",` +
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
		`}}`)

	out, err := V1SilenceID{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}

func TestV1SilenceID_InvalidDifferentIDs(t *testing.T) {
	in := [][]byte{
		[]byte(`{"status":"success","data":{` +
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
			`"updatedAt":"2021-04-28T17:32:01.725956017Z",` +
			`"createdBy":"",` +
			`"comment":"Silence Comment #1",` +
			`"status":{"state":"active"}` +
			`}}`),
		[]byte(`{"status":"success","data":{` +
			`"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b",` +
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
			`"comment":"Silence Comment #2",` +
			`"status":{"state":"active"}` +
			`}}`),
	}

	_, err := V1SilenceID{}.MergeResponses(in)
	require.Error(t, err)
}
