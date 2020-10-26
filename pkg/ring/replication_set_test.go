package ring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplicationSet_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		rs       ReplicationSet
		expected []string
	}{
		"should return an empty slice on empty replication set": {
			rs:       ReplicationSet{},
			expected: []string{},
		},
		"should return instances addresses (no order guaranteed)": {
			rs: ReplicationSet{
				Ingesters: []IngesterDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, testData.rs.GetAddresses())
		})
	}
}

// Return a function that fails starting from failAfter times
func failingFunctionAfter(failAfter int, delay time.Duration) func(context.Context, *IngesterDesc) (interface{}, error) {
	count := 0
	return func(context.Context, *IngesterDesc) (interface{}, error) {
		count++
		time.Sleep(delay)
		if count > failAfter {
			return nil, errors.New("Dummy error")
		}
		return 1, nil
	}
}

func TestReplicationSet_Do(t *testing.T) {
	tests := []struct {
		name              string
		ingesters         []IngesterDesc
		maxErrors         int
		f                 func(context.Context, *IngesterDesc) (interface{}, error)
		delay             time.Duration
		closeContextDelay time.Duration
		want              []interface{}
		wantErr           bool
		expectedError     error
	}{
		{
			name: "no errors no delay",
			ingesters: []IngesterDesc{
				{},
			},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1},
		},
		{
			name:      "1 error, no errors expected",
			ingesters: []IngesterDesc{{}},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return nil, errors.New("Dummy error")
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:      "3 ingesters, last call fails",
			ingesters: []IngesterDesc{{}, {}, {}},
			f:         failingFunctionAfter(2, 10*time.Millisecond),
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "5 ingesters, with delay, last 3 calls fail",
			ingesters: []IngesterDesc{{}, {}, {}, {}, {}},
			maxErrors: 1,
			f:         failingFunctionAfter(2, 10*time.Millisecond),
			delay:     100 * time.Millisecond,
			want:      nil,
			wantErr:   true,
		},
		{
			name:              "3 ingesters, context fails",
			ingesters:         []IngesterDesc{{}, {}, {}},
			maxErrors:         1,
			f:                 failingFunctionAfter(0, 200*time.Millisecond),
			closeContextDelay: 100 * time.Millisecond,
			want:              nil,
			wantErr:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ReplicationSet{
				Ingesters: tt.ingesters,
				MaxErrors: tt.maxErrors,
			}
			ctx := context.Background()
			if tt.closeContextDelay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.AfterFunc(tt.closeContextDelay, func() {
						cancel()
					})
				}()
			}
			got, err := r.Do(ctx, tt.delay, tt.f)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedError != nil {
					assert.Equal(t, tt.expectedError, err)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
