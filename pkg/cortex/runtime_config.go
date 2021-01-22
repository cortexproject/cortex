package cortex

import (
	"errors"
	"io"
	"net/http"

	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errMultipleDocuments = errors.New("the provided runtime configuration contains multiple documents")
)

// runtimeConfigValues are values that can be reloaded from configuration file while Cortex is running.
// Reloading is done by runtime_config.Manager, which also keeps the currently loaded config.
// These values are then pushed to the components that are interested in them.
type runtimeConfigValues struct {
	TenantLimits map[string]*validation.Limits `yaml:"overrides"`

	Multi kv.MultiRuntimeConfig `yaml:"multi_kv_config"`
}

func loadRuntimeConfig(r io.Reader) (interface{}, error) {
	var overrides = &runtimeConfigValues{}

	decoder := yaml.NewDecoder(r)
	decoder.SetStrict(true)

	// Decode the first document. An empty document (EOF) is OK.
	if err := decoder.Decode(&overrides); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	// Ensure the provided YAML config is not composed of multiple documents,
	if err := decoder.Decode(&runtimeConfigValues{}); !errors.Is(err, io.EOF) {
		return nil, errMultipleDocuments
	}

	return overrides, nil
}

func tenantLimitsFromRuntimeConfig(c *runtimeconfig.Manager) validation.TenantLimits {
	if c == nil {
		return nil
	}
	return func(userID string) *validation.Limits {
		cfg, ok := c.GetConfig().(*runtimeConfigValues)
		if !ok || cfg == nil {
			return nil
		}

		return cfg.TenantLimits[userID]
	}
}

func multiClientRuntimeConfigChannel(manager *runtimeconfig.Manager) func() <-chan kv.MultiRuntimeConfig {
	if manager == nil {
		return nil
	}
	// returns function that can be used in MultiConfig.ConfigProvider
	return func() <-chan kv.MultiRuntimeConfig {
		outCh := make(chan kv.MultiRuntimeConfig, 1)

		// push initial config to the channel
		val := manager.GetConfig()
		if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
			outCh <- cfg.Multi
		}

		ch := manager.CreateListenerChannel(1)
		go func() {
			for val := range ch {
				if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
					outCh <- cfg.Multi
				}
			}
		}()

		return outCh
	}
}

func diffLimitsConfig(defaultConfig, actualConfig map[interface{}]interface{}) (map[interface{}]interface{}, error) {
	output := make(map[interface{}]interface{})
	for tenant, tenantValues := range actualConfig {
		tenantValuesObj, err := util.YAMLMarshalUnmarshal(tenantValues)
		if err != nil {
			return nil, err
		}
		tenantDiff, err := util.DiffConfig(defaultConfig, tenantValuesObj)
		if err != nil {
			return nil, err
		}
		output[tenant] = tenantDiff
	}
	return output, nil
}

func runtimeConfigHandler(runtimeCfgManager *runtimeconfig.Manager, defaultLimits validation.Limits) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var output interface{}
		runtimeConfig := runtimeCfgManager.GetConfig()
		if runtimeConfig == nil {
			util.WriteTextResponse(w, "runtime config file doesn't exist")
			return
		}
		switch r.URL.Query().Get("mode") {
		case "diff":
			runtimeCfgObj, err := util.YAMLMarshalUnmarshal(runtimeConfig)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if runtimeCfgObj["overrides"] != nil {
				defaultLimitsObj, err := util.YAMLMarshalUnmarshal(defaultLimits)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				limitsCfgObj, err := util.YAMLMarshalUnmarshal(runtimeCfgObj["overrides"])
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				limitsDiff, err := diffLimitsConfig(defaultLimitsObj, limitsCfgObj)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				runtimeCfgObj["overrides"] = limitsDiff
			}
			if runtimeCfgObj["multi_kv_config"] != nil {
				defaultMultiKVObj, err := util.YAMLMarshalUnmarshal(&kv.MultiRuntimeConfig{})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				multiKVCfgObj, err := util.YAMLMarshalUnmarshal(runtimeCfgObj["multi_kv_config"])
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				multKVDiff, err := util.DiffConfig(defaultMultiKVObj, multiKVCfgObj)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				runtimeCfgObj["multi_kv_config"] = multKVDiff
			}
			output = runtimeCfgObj
		default:
			output = runtimeConfig
		}
		util.WriteYAMLResponse(w, output)
	}
}
