package merger

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

func TestV2Alerts(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2Alerts. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{
		[]byte(`[` +
			`{"annotations":{},"endsAt":"2021-04-21T10:47:32.161+02:00","fingerprint":"c4b6b79a607b6ba0",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.161+02:00",` +
			`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.163Z","labels":{"group":"group_1","name":"alert_1"}},` +
			`{"annotations":{},"endsAt":"2021-04-21T10:47:32.163+02:00","fingerprint":"c4b8b79a607bee77",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.163+02:00",` +
			`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.165Z","labels":{"group":"group_1","name":"alert_2"}}` +
			`]`),
		[]byte(`[{"annotations":{},"endsAt":"2021-04-21T10:47:32.165+02:00","fingerprint":"465de60f606461c3",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.165+02:00",` +
			`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.167Z","labels":{"group":"group_2","name":"alert_3"}}]`),
		[]byte(`[]`),
	}

	expected := []byte(`[` +
		`{"annotations":{},"endsAt":"2021-04-21T10:47:32.165+02:00","fingerprint":"465de60f606461c3",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.165+02:00",` +
		`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.167Z","labels":{"group":"group_2","name":"alert_3"}},` +
		`{"annotations":{},"endsAt":"2021-04-21T10:47:32.161+02:00","fingerprint":"c4b6b79a607b6ba0",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.161+02:00",` +
		`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.163Z","labels":{"group":"group_1","name":"alert_1"}},` +
		`{"annotations":{},"endsAt":"2021-04-21T10:47:32.163+02:00","fingerprint":"c4b8b79a607bee77",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.163+02:00",` +
		`"status":{"inhibitedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.165Z","labels":{"group":"group_1","name":"alert_2"}}` +
		`]`)

	out, err := V2Alerts{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, expected, out)
}

func v2ParseTime(s string) *strfmt.DateTime {
	t, _ := time.Parse(time.RFC3339, s)
	dt := strfmt.DateTime(t)
	return &dt
}

// v2alert is a convenience function to create alert structures with certain important fields set
// and with sensible defaults for the remaining fields to test they are passed through.
func v2alert(fingerprint, annotation, updatedAt string) *v2_models.GettableAlert {
	receiver := "dummy"
	return &v2_models.GettableAlert{
		Annotations: v2_models.LabelSet{
			"annotation1": annotation,
		},
		EndsAt:      v2ParseTime("2020-01-01T12:00:00.000Z"),
		Fingerprint: &fingerprint,
		Receivers: []*v2_models.Receiver{
			{
				Name: &receiver,
			},
		},
		StartsAt:  v2ParseTime("2020-01-01T12:00:00.000Z"),
		Status:    &v2_models.AlertStatus{},
		UpdatedAt: v2ParseTime(updatedAt),
		Alert: v2_models.Alert{
			GeneratorURL: strfmt.URI("something"),
			Labels:       v2_models.LabelSet{"label1": "foo"},
		},
	}
}

func v2alerts(alerts ...*v2_models.GettableAlert) v2_models.GettableAlerts {
	return alerts
}

func TestMergeV2Alerts(t *testing.T) {
	var (
		alert1      = v2alert("1111111111111111", "a1-", "2020-01-01T12:00:00.000Z")
		newerAlert1 = v2alert("1111111111111111", "a1+x", "2020-01-01T12:00:00.001Z")
		alert2      = v2alert("2222222222222222", "a2-", "2020-01-01T12:00:00.000Z")
		alert3      = v2alert("3333333333333333", "a3-", "2020-01-01T12:00:00.000Z")
	)
	cases := []struct {
		name string
		in   v2_models.GettableAlerts
		err  error
		out  v2_models.GettableAlerts
	}{
		{
			name: "no alerts, should return an empty list",
			in:   v2alerts(),
			out:  v2_models.GettableAlerts{},
		},
		{
			name: "one alert, should return the alert",
			in:   v2alerts(alert1),
			out:  v2alerts(alert1),
		},
		{
			name: "two alerts, should return two alerts",
			in:   v2alerts(alert1, alert2),
			out:  v2alerts(alert1, alert2),
		},
		{
			name: "three alerts, should return three alerts",
			in:   v2alerts(alert1, alert2, alert3),
			out:  v2alerts(alert1, alert2, alert3),
		},
		{
			name: "three alerts out of order, should return three alerts in fingerprint order",
			in:   v2alerts(alert3, alert2, alert1),
			out:  v2alerts(alert1, alert2, alert3),
		},
		{
			name: "two identical alerts, should return one alert",
			in:   v2alerts(alert1, alert1),
			out:  v2alerts(alert1),
		},
		{
			name: "two identical alerts plus another, should return two alerts",
			in:   v2alerts(alert1, alert1, alert2),
			out:  v2alerts(alert1, alert2),
		},
		{
			name: "two duplicates out of sync alerts, should return newer alert",
			in:   v2alerts(alert1, newerAlert1),
			out:  v2alerts(newerAlert1),
		},
		{
			name: "two duplicates out of sync alerts (newer first), should return newer alert",
			in:   v2alerts(newerAlert1, alert1),
			out:  v2alerts(newerAlert1),
		},
		{
			name: "two duplicates plus others, should return newer alert and others",
			in:   v2alerts(newerAlert1, alert3, alert1, alert2),
			out:  v2alerts(newerAlert1, alert2, alert3),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeV2Alerts(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
