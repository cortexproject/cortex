package rules

import (
	"context"
	fmt "fmt"
	"testing"
	time "time"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/stretchr/testify/assert"
)

type MockClient struct {
	cfgs map[string]configs.VersionedRulesConfig
	err  error
}

func (c *MockClient) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	return c.cfgs, c.err
}

func (c *MockClient) GetAlerts(ctx context.Context, since configs.ID) (*client.ConfigsResponse, error) {
	return nil, nil
}

func Test_ConfigRuleStoreError(t *testing.T) {
	mock := &MockClient{
		cfgs: nil,
		err:  fmt.Errorf("Error"),
	}

	store := NewConfigRuleStore(mock)
	_, err := store.ListAllRuleGroups(nil)

	assert.Equal(t, mock.err, err, "Unexpected error returned")
}

func Test_ConfigRuleStoreReturn(t *testing.T) {
	id := configs.ID(10)
	mock := &MockClient{
		cfgs: map[string]configs.VersionedRulesConfig{
			"user": {
				ID:        id,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
		},
		err: nil,
	}

	store := NewConfigRuleStore(mock)
	rules, _ := store.ListAllRuleGroups(nil)

	assert.Equal(t, 1, len(rules["user"]))
	assert.Equal(t, id, store.since)
}

func Test_ConfigRuleStoreDelete(t *testing.T) {
	mock := &MockClient{
		cfgs: map[string]configs.VersionedRulesConfig{
			"user": {
				ID:        1,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
		},
		err: nil,
	}

	store := NewConfigRuleStore(mock)
	store.ListAllRuleGroups(nil)

	mock.cfgs["user"] = configs.VersionedRulesConfig{
		ID:        1,
		Config:    configs.RulesConfig{},
		DeletedAt: time.Unix(0, 1),
	}

	rules, _ := store.ListAllRuleGroups(nil)

	assert.Equal(t, 0, len(rules["user"]))
}

func Test_ConfigRuleStoreAppend(t *testing.T) {
	mock := &MockClient{
		cfgs: map[string]configs.VersionedRulesConfig{
			"user": {
				ID:        1,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
		},
		err: nil,
	}

	store := NewConfigRuleStore(mock)
	store.ListAllRuleGroups(nil)

	delete(mock.cfgs, "user")
	mock.cfgs["user2"] = configs.VersionedRulesConfig{
		ID:        1,
		Config:    fakeRuleConfig(),
		DeletedAt: time.Unix(0, 0),
	}

	rules, _ := store.ListAllRuleGroups(nil)

	assert.Equal(t, 2, len(rules))
}

func Test_ConfigRuleStoreSinceSet(t *testing.T) {
	mock := &MockClient{
		cfgs: map[string]configs.VersionedRulesConfig{
			"user": {
				ID:        1,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
			"user1": {
				ID:        10,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
			"user2": {
				ID:        100,
				Config:    fakeRuleConfig(),
				DeletedAt: time.Unix(0, 0),
			},
		},
		err: nil,
	}

	store := NewConfigRuleStore(mock)
	store.ListAllRuleGroups(nil)
	assert.Equal(t, configs.ID(100), store.since)

	delete(mock.cfgs, "user")
	delete(mock.cfgs, "user1")
	mock.cfgs["user2"] = configs.VersionedRulesConfig{
		ID:        50,
		Config:    fakeRuleConfig(),
		DeletedAt: time.Unix(0, 0),
	}

	store.ListAllRuleGroups(nil)
	assert.Equal(t, configs.ID(100), store.since)

	mock.cfgs["user2"] = configs.VersionedRulesConfig{
		ID:        101,
		Config:    fakeRuleConfig(),
		DeletedAt: time.Unix(0, 0),
	}

	store.ListAllRuleGroups(nil)
	assert.Equal(t, configs.ID(101), store.since)
}

func fakeRuleConfig() configs.RulesConfig {
	return configs.RulesConfig{
		FormatVersion: configs.RuleFormatV2,
		Files: map[string]string{
			"test": `
# Config no. 1.
groups:
- name: example
  rules:
  - alert: ScrapeFailed
    expr: 'up != 1'
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Scrape of {{$labels.job}} (pod: {{$labels.instance}}) failed."
      description: "Prometheus cannot reach the /metrics page on the {{$labels.instance}} pod."
      impact: "We have no monitoring data for {{$labels.job}} - {{$labels.instance}}. At worst, it's completely down. At best, we cannot reliably respond to operational issues."
      dashboardURL: "$${base_url}/admin/prometheus/targets"
`,
		},
	}
}
