package migrate

import (
	"flag"
	"fmt"
	"strings"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// PlanConfig is used to configure the Planner
type PlanConfig struct {
	FirstShard int
	LastShard  int
	BatchSize  int
	UserIDList string
	Tables     string
}

// Notes on Planned Shards
// #######################
// When doing migrations each database is discreetly partitioned into 240 shards
// based on aspects of the databases underlying implementation. 240 was chosen due
// to the bigtable implementation sharding on the first two character of the hex encoded
// metric fingerprint. Cassandra is encoded into 240 discreet shards using the Murmur3
// partition tokens.

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *PlanConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.FirstShard, "plan.firstShard", 0, "fist shard in range of shards to be migrated (0-240)")
	f.IntVar(&cfg.LastShard, "plan.lastShard", 240, "last shard in range of shards to be migrated (0-240)")
	f.IntVar(&cfg.BatchSize, "plan.batchsize", 1, "number of shards to stream per batch")
	f.StringVar(&cfg.UserIDList, "plan.users", "", "comma separated list of user ids, if empty all users will be queried")
	f.StringVar(&cfg.Tables, "plan.tables", "", "comma separated list of tables to migrate")
}

// Planner plans the queries required for the migration
type Planner struct {
	firstShard int
	lastShard  int
	batchSize  int
	tables     []string
	users      []string
}

// NewPlanner returns a new planner struct
func NewPlanner(cfg PlanConfig) (Planner, error) {
	if cfg.FirstShard < 0 || cfg.FirstShard > 240 {
		return Planner{}, fmt.Errorf("plan.firstShard set to %v, must be in range 0-240", cfg.FirstShard)
	}
	if cfg.LastShard < 0 || cfg.LastShard > 240 {
		return Planner{}, fmt.Errorf("plan.lastShard set to %v, must be in range 0-240", cfg.LastShard)
	}
	if cfg.FirstShard > cfg.LastShard {
		return Planner{}, fmt.Errorf("plan.lastShard (%v) is set to less than plan.from (%v)", cfg.LastShard, cfg.FirstShard)
	}

	userList := strings.Split(cfg.UserIDList, ",")
	tableList := strings.Split(cfg.Tables, ",")
	return Planner{
		firstShard: cfg.FirstShard,
		lastShard:  cfg.LastShard,
		users:      userList,
		tables:     tableList,
		batchSize:  cfg.BatchSize,
	}, nil
}

// Plan updates a StreamBatch with the correct queries for the planned migration
func (p Planner) Plan(batch chunk.StreamBatch) {
	for _, table := range p.tables {
		for _, user := range p.users {
			for from := p.firstShard; from < p.lastShard; {
				to := from + p.batchSize
				if to > p.lastShard {
					batch.Add(table, user, from, p.lastShard)
				} else {
					batch.Add(table, user, from, to)
				}
				from = to
			}
		}
	}
}
