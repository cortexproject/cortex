package ring

import (
	"context"
	"errors"
	"sync"
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
	var mutex = &sync.RWMutex{}
	count := 0
	return func(context.Context, *IngesterDesc) (interface{}, error) {
		mutex.Lock()
		count++
		mutex.Unlock()
		time.Sleep(delay)
		mutex.RLock()
		defer mutex.RUnlock()
		if count > failAfter {
			return nil, errors.New("Dummy error")
		}
		return 1, nil
	}
}

func failingFunctionForZones(zones ...string) func(context.Context, *IngesterDesc) (interface{}, error) {
	return func(ctx context.Context, ing *IngesterDesc) (interface{}, error) {
		for _, zone := range zones {
			if ing.Zone == zone {
				return nil, errors.New("Failed")
			}
		}
		return 1, nil
	}
}

func TestReplicationSet_Do(t *testing.T) {
	tests := []struct {
		name                string
		ingesters           []IngesterDesc
		maxErrors           int
		maxUnavailableZones int
		f                   func(context.Context, *IngesterDesc) (interface{}, error)
		delay               time.Duration
		closeContextDelay   time.Duration
		want                []interface{}
		wantErr             bool
		expectedError       error
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
		{
			name: "3 ingesters, 3 zones, no failures",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1, 1, 1},
		},
		{
			name: "3 ingesters, 3 zones, 1 zone fails",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 1, // (nr of zones) / 2
			maxErrors:           1, // (nr of ingesters / nr of zones) * maxUnavailableZones
			want:                []interface{}{1, 1},
		},
		{
			name: "3 ingesters, 3 zones, 2 zones fail",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1", "zone2"),
			maxUnavailableZones: 1,
			maxErrors:           1,
			wantErr:             true,
			expectedError:       errorTooManyZoneFailures,
		},
		{
			name: "6 ingesters, 3 zones, 1 zone fails",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 1,
			maxErrors:           2,
			want:                []interface{}{1, 1, 1, 1},
		},
		{
			name: "5 ingesters, 5 zones, 3 zones fails",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone5",
			}},
			f:                   failingFunctionForZones("zone1", "zone2", "zone3"),
			maxUnavailableZones: 2,
			maxErrors:           2,
			wantErr:             true,
			expectedError:       errorTooManyZoneFailures,
		},
		{
			name: "10 ingesters, 5 zones, 2 failures in zone 1",
			ingesters: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone5",
			}, {
				Zone: "zone5",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 2,
			maxErrors:           4,
			want:                []interface{}{1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ReplicationSet{
				Ingesters:           tt.ingesters,
				MaxErrors:           tt.maxErrors,
				MaxUnavailableZones: tt.maxUnavailableZones,
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
