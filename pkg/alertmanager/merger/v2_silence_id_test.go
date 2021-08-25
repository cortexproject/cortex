package merger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV2SilenceId_ReturnsNewestSilence(t *testing.T) {

	// We re-use MergeV2Silences so we rely on that being primarily tested elsewhere.

	in := [][]byte{
		[]byte(`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.215Z","comment":"This is the newest silence",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.725Z"}`),
		[]byte(`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.000Z","comment":"Silence Comment #1",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.725Z"}`),
		[]byte(`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.000Z","comment":"Silence Comment #1",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.725Z"}`),
	}

	expected := []byte(`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
		`"updatedAt":"2021-04-28T17:31:02.215Z","comment":"This is the newest silence",` +
		`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
		`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
		`"startsAt":"2021-04-28T17:31:01.725Z"}`)

	out, err := V2SilenceID{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}

func TestV2SilenceID_InvalidDifferentIDs(t *testing.T) {

	// Responses containing silences with different IDs is invalid input.

	in := [][]byte{
		[]byte(`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.215Z","comment":"Silence Comment #1",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.725Z"}`),
		[]byte(`{"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.082Z","comment":"Silence Comment #3",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.082Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.735Z"}`),
	}

	_, err := V2SilenceID{}.MergeResponses(in)
	require.Error(t, err)
}
