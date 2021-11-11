package merger

import (
	"testing"
	"time"

	v1 "github.com/prometheus/alertmanager/api/v1"
	"github.com/prometheus/alertmanager/types"
	prom_model "github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestV1Alerts(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV1Alerts. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{
		[]byte(`{"status":"success","data":[` +
			`{"labels":{"group":"group_1","name":"alert_1"},` +
			`"annotations":null,"startsAt":"2021-04-21T09:47:32.16145934+02:00",` +
			`"endsAt":"2021-04-21T10:47:32.161462204+02:00","generatorURL":"",` +
			`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
			`"receivers":["dummy"],"fingerprint":"c4b6b79a607b6ba0"},` +
			`{"labels":{"group":"group_1","name":"alert_2"},` +
			`"annotations":null,"startsAt":"2021-04-21T09:47:32.163797336+02:00",` +
			`"endsAt":"2021-04-21T10:47:32.163800129+02:00","generatorURL":"",` +
			`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
			`"receivers":["dummy"],"fingerprint":"c4b8b79a607bee77"}]}`),
		[]byte(`{"status":"success","data":[` +
			`{"labels":{"group":"group_2","name":"alert_3"},` +
			`"annotations":null,"startsAt":"2021-04-21T09:47:32.165939585+02:00",` +
			`"endsAt":"2021-04-21T10:47:32.165942448+02:00","generatorURL":"",` +
			`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
			`"receivers":["dummy"],"fingerprint":"465de60f606461c3"}]}`),
		[]byte(`{"status":"success","data":[]}`),
	}

	expected := []byte(`{"status":"success","data":[` +
		`{"labels":{"group":"group_2","name":"alert_3"},"annotations":null,` +
		`"startsAt":"2021-04-21T09:47:32.165939585+02:00",` +
		`"endsAt":"2021-04-21T10:47:32.165942448+02:00","generatorURL":"",` +
		`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
		`"receivers":["dummy"],"fingerprint":"465de60f606461c3"},` +
		`{"labels":{"group":"group_1","name":"alert_1"},"annotations":null,` +
		`"startsAt":"2021-04-21T09:47:32.16145934+02:00",` +
		`"endsAt":"2021-04-21T10:47:32.161462204+02:00","generatorURL":"",` +
		`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
		`"receivers":["dummy"],"fingerprint":"c4b6b79a607b6ba0"},` +
		`{"labels":{"group":"group_1","name":"alert_2"},"annotations":null,` +
		`"startsAt":"2021-04-21T09:47:32.163797336+02:00",` +
		`"endsAt":"2021-04-21T10:47:32.163800129+02:00","generatorURL":"",` +
		`"status":{"state":"unprocessed","silencedBy":[],"inhibitedBy":[]},` +
		`"receivers":["dummy"],"fingerprint":"c4b8b79a607bee77"}]}`)

	out, err := V1Alerts{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, expected, out)
}

func v1ParseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// v1alert is a convenience function to create alert structures with certain important fields set
// and with sensible defaults for the remaining fields to test they are passed through.
func v1alert(fingerprint, annotation string) *v1.Alert {
	return &v1.Alert{
		Alert: &prom_model.Alert{
			Labels: prom_model.LabelSet{
				"label1": "value1",
			},
			Annotations: prom_model.LabelSet{
				"annotation1": prom_model.LabelValue(annotation),
			},
			StartsAt:     v1ParseTime("2020-01-01T12:00:00.000Z"),
			EndsAt:       v1ParseTime("2020-01-01T12:00:00.000Z"),
			GeneratorURL: "something",
		},
		Status:      types.AlertStatus{},
		Receivers:   []string{"dummy"},
		Fingerprint: fingerprint,
	}
}

func v1alerts(alerts ...*v1.Alert) []*v1.Alert {
	return alerts
}

func TestMergeV1Alerts(t *testing.T) {
	var (
		alert1  = v1alert("1111111111111111", "a1")
		alert1b = v1alert("1111111111111111", "a1b")
		alert2  = v1alert("2222222222222222", "a2")
		alert3  = v1alert("3333333333333333", "a3")
	)
	cases := []struct {
		name string
		in   []*v1.Alert
		err  error
		out  []*v1.Alert
	}{
		{
			name: "no alerts, should return an empty list",
			in:   v1alerts(),
			out:  []*v1.Alert{},
		},
		{
			name: "one alert, should return the alert",
			in:   v1alerts(alert1),
			out:  v1alerts(alert1),
		},
		{
			name: "two alerts, should return two alerts",
			in:   v1alerts(alert1, alert2),
			out:  v1alerts(alert1, alert2),
		},
		{
			name: "three alerts, should return three alerts",
			in:   v1alerts(alert1, alert2, alert3),
			out:  v1alerts(alert1, alert2, alert3),
		},
		{
			name: "three alerts out of order, should return three alerts in fingerprint order",
			in:   v1alerts(alert3, alert2, alert1),
			out:  v1alerts(alert1, alert2, alert3),
		},
		{
			name: "two identical alerts, should return one alert",
			in:   v1alerts(alert1, alert1),
			out:  v1alerts(alert1),
		},
		{
			name: "two identical alerts plus another, should return two alerts",
			in:   v1alerts(alert1, alert1, alert2),
			out:  v1alerts(alert1, alert2),
		},
		{
			name: "two duplicates out of sync alerts, should return first seen alert",
			in:   v1alerts(alert1, alert1b),
			out:  v1alerts(alert1),
		},
		{
			name: "two duplicates plus others, should return first seen alert and others",
			in:   v1alerts(alert1b, alert3, alert1, alert2),
			out:  v1alerts(alert1b, alert2, alert3),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeV1Alerts(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
