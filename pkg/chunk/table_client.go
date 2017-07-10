package chunk

import "golang.org/x/net/context"

// TableClient is a client for telling Dynamo what to do with tables.
type TableClient interface {
	ListTables(ctx context.Context) ([]string, error)
	CreateTable(ctx context.Context, desc TableDesc) error
	DescribeTable(ctx context.Context, name string) (desc TableDesc, status string, err error)
	UpdateTable(ctx context.Context, current, expected TableDesc) error
}

// TableDesc describes a table.
type TableDesc struct {
	Name             string
	ProvisionedRead  int64
	ProvisionedWrite int64
	Tags             Tags

	WriteScaleEnabled     bool
	WriteScaleRoleARN     string
	WriteScaleMinCapacity int64
	WriteScaleMaxCapacity int64
	WriteScaleOutCooldown int64
	WriteScaleInCooldown  int64
	WriteScaleTargetValue float64
}

// Equals returns true if other matches desc.
func (desc TableDesc) Equals(other TableDesc) bool {
	if !desc.AutoScalingEquals(other) {
		return false
	}

	if desc.ProvisionedRead != other.ProvisionedRead {
		return false
	}

	// Only check provisioned write if auto scaling is disabled
	if !desc.WriteScaleEnabled && desc.ProvisionedWrite != other.ProvisionedWrite {
		return false
	}

	if !desc.Tags.Equals(other.Tags) {
		return false
	}

	return true
}

// AutoScalingEquals returns true if auto scaling is equal
func (desc TableDesc) AutoScalingEquals(other TableDesc) bool {
	if desc.WriteScaleEnabled != other.WriteScaleEnabled {
		return false
	}

	if desc.WriteScaleEnabled {
		if desc.WriteScaleRoleARN != other.WriteScaleRoleARN {
			return false
		}

		if desc.WriteScaleMinCapacity != other.WriteScaleMinCapacity {
			return false
		}

		if desc.WriteScaleMaxCapacity != other.WriteScaleMaxCapacity {
			return false
		}

		if desc.WriteScaleOutCooldown != other.WriteScaleOutCooldown {
			return false
		}

		if desc.WriteScaleInCooldown != other.WriteScaleInCooldown {
			return false
		}

		if desc.WriteScaleTargetValue != other.WriteScaleTargetValue {
			return false
		}
	}
	return true
}

type byName []TableDesc

func (a byName) Len() int           { return len(a) }
func (a byName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool { return a[i].Name < a[j].Name }
