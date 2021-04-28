package merger

import (
	"testing"

	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

func TestV2Silences(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2Silences. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{
		[]byte(`[` +
			`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.215Z","comment":"Silence Comment #1",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.725Z"},` +
			`{"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:02.082Z","comment":"Silence Comment #3",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:02.082Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.735Z"}` +
			`]`),
		[]byte(`[` +
			`{"id":"17526003-c745-4464-a355-4f06de26a236","status":{"state":"expired"},` +
			`"updatedAt":"2021-04-28T17:31:01.953Z","comment":"Silence Comment #2",` +
			`"createdBy":"","endsAt":"2021-04-28T17:31:01.953Z","matchers":` +
			`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
			`"startsAt":"2021-04-28T17:31:01.731Z"}` +
			`]`),
		[]byte(`[]`),
	}

	expected := []byte(`[` +
		`{"id":"77b580dd-1d9c-4b7e-9bba-13ac173cb4e5","status":{"state":"expired"},` +
		`"updatedAt":"2021-04-28T17:31:02.215Z","comment":"Silence Comment #1",` +
		`"createdBy":"","endsAt":"2021-04-28T17:31:02.215Z","matchers":` +
		`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
		`"startsAt":"2021-04-28T17:31:01.725Z"},` +
		`{"id":"261248d1-4ff7-4cf1-9957-850c65f4e48b","status":{"state":"expired"},` +
		`"updatedAt":"2021-04-28T17:31:02.082Z","comment":"Silence Comment #3",` +
		`"createdBy":"","endsAt":"2021-04-28T17:31:02.082Z","matchers":` +
		`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
		`"startsAt":"2021-04-28T17:31:01.735Z"},` +
		`{"id":"17526003-c745-4464-a355-4f06de26a236","status":{"state":"expired"},` +
		`"updatedAt":"2021-04-28T17:31:01.953Z","comment":"Silence Comment #2",` +
		`"createdBy":"","endsAt":"2021-04-28T17:31:01.953Z","matchers":` +
		`[{"isEqual":true,"isRegex":false,"name":"instance","value":"prometheus-one"}],` +
		`"startsAt":"2021-04-28T17:31:01.731Z"}` +
		`]`)

	out, err := V2Silences{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}

// v2silence is a convenience function to create silence structures with certain important fields set
// and with sensible defaults for the remaining fields to test they are passed through.
func v2silence(id, endsAt, updatedAt string) *v2_models.GettableSilence {
	var (
		active    = v2_models.SilenceStatusStateActive
		comment   = "test"
		createdBy = "someone"
		isEqual   = true
		isRegex   = false
		name      = "foo"
		value     = "bar"
	)
	return &v2_models.GettableSilence{
		ID: &id,
		Status: &v2_models.SilenceStatus{
			State: &active,
		},
		UpdatedAt: v2ParseTime(updatedAt),
		Silence: v2_models.Silence{
			Comment:   &comment,
			CreatedBy: &createdBy,
			EndsAt:    v2ParseTime(endsAt),
			Matchers: v2_models.Matchers{
				&v2_models.Matcher{
					IsEqual: &isEqual,
					IsRegex: &isRegex,
					Name:    &name,
					Value:   &value,
				},
			},
			StartsAt: v2ParseTime("2020-01-01T12:00:00.000Z"),
		},
	}
}

func v2silences(silences ...*v2_models.GettableSilence) v2_models.GettableSilences {
	return silences
}

func TestMergeV2Silences(t *testing.T) {
	var (
		silence1      = v2silence("id1", "2020-01-01T12:11:11.000Z", "2020-01-01T12:00:00.000Z")
		newerSilence1 = v2silence("id1", "2020-01-01T12:11:11.000Z", "2020-01-01T12:00:00.001Z")
		silence2      = v2silence("id2", "2020-01-01T12:22:22.000Z", "2020-01-01T12:00:00.000Z")
		silence3      = v2silence("id3", "2020-01-01T12:33:33.000Z", "2020-01-01T12:00:00.000Z")
	)
	cases := []struct {
		name string
		in   v2_models.GettableSilences
		err  error
		out  v2_models.GettableSilences
	}{
		{
			name: "no silences, should return an empty list",
			in:   v2silences(),
			out:  v2_models.GettableSilences{},
		},
		{
			name: "one silence, should return the silence",
			in:   v2silences(silence1),
			out:  v2silences(silence1),
		},
		{
			name: "two silences, should return two silences",
			in:   v2silences(silence1, silence2),
			out:  v2silences(silence1, silence2),
		},
		{
			name: "three silences, should return three silences",
			in:   v2silences(silence1, silence2, silence3),
			out:  v2silences(silence1, silence2, silence3),
		},
		{
			name: "three active silences out of order, should return three silences in expiry order",
			in:   v2silences(silence3, silence2, silence1),
			out:  v2silences(silence1, silence2, silence3),
		},
		{
			name: "two identical silences, should return one silence",
			in:   v2silences(silence1, silence1),
			out:  v2silences(silence1),
		},
		{
			name: "two identical silences plus another, should return two silences",
			in:   v2silences(silence1, silence1, silence2),
			out:  v2silences(silence1, silence2),
		},
		{
			name: "two duplicates out of sync silences, should return newer silence",
			in:   v2silences(silence1, newerSilence1),
			out:  v2silences(newerSilence1),
		},
		{
			name: "two duplicates out of sync silences (newer first), should return newer silence",
			in:   v2silences(newerSilence1, silence1),
			out:  v2silences(newerSilence1),
		},
		{
			name: "two duplicates plus others, should return newer silence and others",
			in:   v2silences(newerSilence1, silence3, silence1, silence2),
			out:  v2silences(newerSilence1, silence2, silence3),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeV2Silences(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
