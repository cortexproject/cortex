package runtimeconfig

import (
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type RuntimeConfigValues struct {
	TenantLimits map[string]*validation.Limits `yaml:"overrides"`

	Multi kv.MultiRuntimeConfig `yaml:"multi_kv_config"`

	IngesterChunkStreaming *bool `yaml:"ingester_stream_chunks_when_using_blocks"`

	IngesterLimits *ingester.InstanceLimits `yaml:"ingester_limits"`

	HardTenantLimits map[string]*validation.Limits `yaml:"hard_overrides,omitempty"`

	APIAllowedLimits []string `yaml:"api_allowed_limits,omitempty"`
}
