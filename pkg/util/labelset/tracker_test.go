package labelset

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabelSetTracker(t *testing.T) {
	tracker := NewLabelSetTracker()
	userID := "1"
	userID2 := "2"
	userID3 := "3"

	tracker.Track(userID, 0, labels.FromStrings("foo", "bar"))
	require.True(t, tracker.labelSetExists(userID, 0, labels.FromStrings("foo", "bar")))
	tracker.Track(userID, 1, labels.FromStrings("foo", "baz"))
	require.True(t, tracker.labelSetExists(userID, 1, labels.FromStrings("foo", "baz")))
	tracker.Track(userID, 3, labels.EmptyLabels())
	require.True(t, tracker.labelSetExists(userID, 3, labels.EmptyLabels()))
	tracker.Track(userID2, 0, labels.FromStrings("foo", "bar"))
	require.True(t, tracker.labelSetExists(userID2, 0, labels.FromStrings("foo", "bar")))
	tracker.Track(userID2, 2, labels.FromStrings("cluster", "us-west-2"))
	require.True(t, tracker.labelSetExists(userID2, 2, labels.FromStrings("cluster", "us-west-2")))

	// Increment metrics and add a new user.
	tracker.Track(userID, 3, labels.EmptyLabels())
	require.True(t, tracker.labelSetExists(userID, 3, labels.EmptyLabels()))
	tracker.Track(userID2, 2, labels.FromStrings("cluster", "us-west-2"))
	require.True(t, tracker.labelSetExists(userID2, 2, labels.FromStrings("cluster", "us-west-2")))
	tracker.Track(userID2, 4, labels.FromStrings("cluster", "us-west-2"))
	require.True(t, tracker.labelSetExists(userID2, 4, labels.FromStrings("cluster", "us-west-2")))
	tracker.Track(userID3, 4, labels.FromStrings("cluster", "us-east-1"))
	require.True(t, tracker.labelSetExists(userID3, 4, labels.FromStrings("cluster", "us-east-1")))

	// Remove user 2.
	userSet := map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}, 1: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		userID3: {0: struct{}{}, 1: struct{}{}, 2: struct{}{}, 3: struct{}{}, 4: struct{}{}},
	}
	tracker.UpdateMetrics(userSet, noopDeleteMetrics)
	// user 2 removed.
	require.False(t, tracker.userExists(userID2))
	// user 1 and 3 remain unchanged.
	require.True(t, tracker.labelSetExists(userID, 0, labels.FromStrings("foo", "bar")))
	require.True(t, tracker.labelSetExists(userID, 1, labels.FromStrings("foo", "baz")))
	require.True(t, tracker.labelSetExists(userID, 3, labels.EmptyLabels()))
	require.True(t, tracker.labelSetExists(userID3, 4, labels.FromStrings("cluster", "us-east-1")))

	// Simulate existing limits removed for each user.
	userSet = map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}},
		userID2: {},
		userID3: {},
	}
	tracker.UpdateMetrics(userSet, noopDeleteMetrics)
	// User 2 and 3 removed. User 1 exists.
	require.True(t, tracker.userExists(userID))
	require.False(t, tracker.userExists(userID2))
	require.False(t, tracker.userExists(userID3))

	require.True(t, tracker.labelSetExists(userID, 0, labels.FromStrings("foo", "bar")))
	require.False(t, tracker.labelSetExists(userID, 1, labels.FromStrings("foo", "baz")))
	require.False(t, tracker.labelSetExists(userID, 3, labels.EmptyLabels()))
	require.False(t, tracker.labelSetExists(userID3, 4, labels.FromStrings("cluster", "us-east-1")))
}

func noopDeleteMetrics(user, labelSetStr string, removeUser bool) {}

func TestLabelSetTracker_UpdateMetrics(t *testing.T) {
	tracker := NewLabelSetTracker()
	userID := "1"
	userID2 := "2"
	userID3 := "3"
	lbls := labels.FromStrings("foo", "bar")
	lbls2 := labels.FromStrings("foo", "baz")
	tracker.Track(userID, 0, lbls)
	tracker.Track(userID, 1, lbls2)
	tracker.Track(userID2, 0, lbls)
	tracker.Track(userID2, 1, lbls2)
	tracker.Track(userID3, 0, lbls)
	tracker.Track(userID3, 1, lbls2)

	deleteCalls := make(map[string]struct{})
	mockDeleteMetrics := func(user, labelSetStr string, removeUser bool) {
		deleteCalls[formatDeleteCallString(user, labelSetStr, removeUser)] = struct{}{}
	}

	userSet := map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}, 1: struct{}{}},
		userID2: {0: struct{}{}, 1: struct{}{}},
		userID3: {0: struct{}{}, 1: struct{}{}},
	}
	// No user or label set removed, no change.
	tracker.UpdateMetrics(userSet, mockDeleteMetrics)
	require.Equal(t, 0, len(deleteCalls))

	userSet = map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}, 1: struct{}{}},
		userID2: {1: struct{}{}},
		userID3: {0: struct{}{}},
	}
	// LabelSet removed from user 2 and 3
	tracker.UpdateMetrics(userSet, mockDeleteMetrics)
	require.Equal(t, 2, len(deleteCalls))
	_, ok := deleteCalls[formatDeleteCallString(userID2, lbls.String(), false)]
	require.True(t, ok)
	_, ok = deleteCalls[formatDeleteCallString(userID3, lbls2.String(), false)]
	require.True(t, ok)

	userSet = map[string]map[uint64]struct {
	}{
		userID:  {0: struct{}{}, 1: struct{}{}},
		userID2: {1: struct{}{}},
		userID3: {},
	}
	// User 3 doesn't have limits anymore. Remove user.
	tracker.UpdateMetrics(userSet, mockDeleteMetrics)
	require.Equal(t, 3, len(deleteCalls))
	_, ok = deleteCalls[formatDeleteCallString(userID3, "", true)]
	require.True(t, ok)

	userSet = map[string]map[uint64]struct {
	}{
		userID3: {},
	}
	// Remove user 1 and 2.
	tracker.UpdateMetrics(userSet, mockDeleteMetrics)
	require.Equal(t, 5, len(deleteCalls))
	_, ok = deleteCalls[formatDeleteCallString(userID, "", true)]
	require.True(t, ok)
	_, ok = deleteCalls[formatDeleteCallString(userID2, "", true)]
	require.True(t, ok)
}

func formatDeleteCallString(user, labelSetStr string, removeUser bool) string {
	return fmt.Sprintf("%s,%s,%s", user, labelSetStr, strconv.FormatBool(removeUser))
}
