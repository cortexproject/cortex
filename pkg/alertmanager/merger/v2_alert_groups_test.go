package merger

import (
	"testing"

	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

func TestV2AlertGroups(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2AlertGroups. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{

		[]byte(`[` +
			`{"alerts":[{"annotations":{},"endsAt":"2021-04-21T10:47:32.161+02:00","fingerprint":"c4b6b79a607b6ba0",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.161+02:00",` +
			`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.163Z","labels":{"group":"group_1","name":"alert_1"}}],` +
			`"labels":{"group":"group_1"},"receiver":{"name":"dummy"},"routeLabels":{}},` +
			`{"alerts":[{"annotations":{},"endsAt":"2021-04-21T10:47:32.165+02:00","fingerprint":"465de60f606461c3",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.165+02:00",` +
			`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.167Z","labels":{"group":"group_2","name":"alert_3"}}],` +
			`"labels":{"group":"group_2"},"receiver":{"name":"dummy"},"routeLabels":{}}` +
			`]`),
		[]byte(`[` +
			`{"alerts":[{"annotations":{},"endsAt":"2021-04-21T10:47:32.163+02:00","fingerprint":"c4b8b79a607bee77",` +
			`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.163+02:00",` +
			`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
			`"updatedAt":"2021-04-21T07:47:32.165Z","labels":{"group":"group_1","name":"alert_2"}}],` +
			`"labels":{"group":"group_1"},"receiver":{"name":"dummy"},"routeLabels":{}}` +
			`]`),
		[]byte(`[]`),
	}

	expected := []byte(`[` +
		`{"alerts":[{"annotations":{},"endsAt":"2021-04-21T10:47:32.161+02:00","fingerprint":"c4b6b79a607b6ba0",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.161+02:00",` +
		`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.163Z","labels":{"group":"group_1","name":"alert_1"}},` +
		`{"annotations":{},"endsAt":"2021-04-21T10:47:32.163+02:00","fingerprint":"c4b8b79a607bee77",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.163+02:00",` +
		`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.165Z","labels":{"group":"group_1","name":"alert_2"}}],` +
		`"labels":{"group":"group_1"},"receiver":{"name":"dummy"},"routeLabels":{}},` +
		`{"alerts":[{"annotations":{},"endsAt":"2021-04-21T10:47:32.165+02:00","fingerprint":"465de60f606461c3",` +
		`"receivers":[{"name":"dummy"}],"startsAt":"2021-04-21T09:47:32.165+02:00",` +
		`"status":{"inhibitedBy":[],"mutedBy":[],"silencedBy":[],"state":"unprocessed"},` +
		`"updatedAt":"2021-04-21T07:47:32.167Z","labels":{"group":"group_2","name":"alert_3"}}],` +
		`"labels":{"group":"group_2"},"receiver":{"name":"dummy"},"routeLabels":{}}]`)

	out, err := V2AlertGroups{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, expected, out)
}

func v2group(label, receiver string, alerts ...*v2_models.GettableAlert) *v2_models.AlertGroup {
	return &v2_models.AlertGroup{
		Alerts:   alerts,
		Labels:   v2_models.LabelSet{"some-label": label},
		Receiver: &v2_models.ReceiverReference{Name: &receiver},
	}
}

// v2routedGroup is v2group with route labels, as returned by routes that set `labels`.
func v2routedGroup(label, receiver, routeLabel string, alerts ...*v2_models.GettableAlert) *v2_models.AlertGroup {
	group := v2group(label, receiver, alerts...)
	group.RouteLabels = v2_models.LabelSet{"owner": routeLabel}
	return group
}

func v2groups(groups ...*v2_models.AlertGroup) v2_models.AlertGroups {
	return groups
}

func TestMergeV2AlertGroups(t *testing.T) {
	var (
		alert1      = v2alert("1111111111111111", "a1-", "2020-01-01T12:00:00.000Z")
		newerAlert1 = v2alert("1111111111111111", "a1+", "2020-01-01T12:00:00.001Z")
		alert2      = v2alert("2222222222222222", "a2-", "2020-01-01T12:00:00.000Z")
		alert3      = v2alert("3333333333333333", "a2-", "2020-01-01T12:00:00.000Z")
	)
	cases := []struct {
		name string
		in   v2_models.AlertGroups
		err  error
		out  v2_models.AlertGroups
	}{
		{
			name: "no groups, should return no groups",
			in:   v2groups(),
			out:  v2_models.AlertGroups{},
		},
		{
			name: "one group with one alert, should return one group",
			in:   v2groups(v2group("g1", "r1", alert1)),
			out:  v2groups(v2group("g1", "r1", alert1)),
		},
		{
			name: "two groups with different labels, should return two groups",
			in:   v2groups(v2group("g1", "r1", alert1), v2group("g2", "r1", alert2)),
			out:  v2groups(v2group("g1", "r1", alert1), v2group("g2", "r1", alert2)),
		},
		{
			name: "two groups with different receiver, should return two groups",
			in:   v2groups(v2group("g1", "r1", alert1), v2group("g1", "r2", alert2)),
			out:  v2groups(v2group("g1", "r1", alert1), v2group("g1", "r2", alert2)),
		},
		{
			name: "two identical groups with different alerts, should return one group with two alerts",
			in:   v2groups(v2group("g1", "r1", alert1), v2group("g1", "r1", alert2)),
			out:  v2groups(v2group("g1", "r1", alert1, alert2)),
		},
		{
			name: "two identical groups with identical alerts, should return one group with one alert",
			in:   v2groups(v2group("g1", "r1", alert1), v2group("g1", "r1", alert1)),
			out:  v2groups(v2group("g1", "r1", alert1)),
		},
		{
			name: "two identical groups with diverged alerts, should return one group with the newer alert",
			in:   v2groups(v2group("g1", "r1", alert1), v2group("g1", "r1", newerAlert1)),
			out:  v2groups(v2group("g1", "r1", newerAlert1)),
		},
		{
			name: "two sets of identical groups with single alerts, should merge all into two groups ",
			in: v2groups(
				v2group("g1", "r1", alert1), v2group("g1", "r1", alert2),
				v2group("g2", "r1", alert1), v2group("g2", "r1", alert3)),
			out: v2groups(
				v2group("g1", "r1", alert1, alert2),
				v2group("g2", "r1", alert1, alert3)),
		},
		{
			name: "two groups with same labels and receiver but different route labels, should return two groups",
			in:   v2groups(v2routedGroup("g1", "r1", "a", alert1), v2routedGroup("g1", "r1", "b", alert2)),
			out:  v2groups(v2routedGroup("g1", "r1", "a", alert1), v2routedGroup("g1", "r1", "b", alert2)),
		},
		{
			name: "same routed groups from multiple replicas, should merge per route",
			in: v2groups(
				v2routedGroup("g1", "r1", "a", alert1), v2routedGroup("g1", "r1", "b", alert2),
				v2routedGroup("g1", "r1", "a", alert1), v2routedGroup("g1", "r1", "b", alert3)),
			out: v2groups(
				v2routedGroup("g1", "r1", "a", alert1),
				v2routedGroup("g1", "r1", "b", alert2, alert3)),
		},
		{
			name: "unordered groups with same labels and receiver, should return groups ordered by route labels",
			in:   v2groups(v2routedGroup("g1", "r1", "b", alert2), v2routedGroup("g1", "r1", "a", alert1)),
			out:  v2groups(v2routedGroup("g1", "r1", "a", alert1), v2routedGroup("g1", "r1", "b", alert2)),
		},
		{
			name: "many unordered groups, should return groups ordered by labels then receiver",
			in: v2groups(
				v2group("g3", "r2", alert3), v2group("g2", "r1", alert2),
				v2group("g1", "r2", alert1), v2group("g1", "r1", alert1),
				v2group("g2", "r2", alert2), v2group("g3", "r1", alert3)),
			out: v2groups(
				v2group("g1", "r1", alert1), v2group("g1", "r2", alert1),
				v2group("g2", "r1", alert2), v2group("g2", "r2", alert2),
				v2group("g3", "r1", alert3), v2group("g3", "r2", alert3)),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeV2AlertGroups(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
