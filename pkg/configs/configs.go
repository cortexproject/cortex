package configs

// An ID is the ID of a single users's Cortex configuration. When a
// configuration changes, it gets a new ID.
type ID int

// A Config is a Cortex configuration for a single user.
type Config struct {
	// RulesFiles maps from a rules filename to file contents.
	RulesFiles         RulesConfig `json:"rules_files"`
	AlertmanagerConfig string      `json:"alertmanager_config"`
}

// View is what's returned from the Weave Cloud configs service
// when we ask for all Cortex configurations.
//
// The configs service is essentially a JSON blob store that gives each
// _version_ of a configuration a unique ID and guarantees that later versions
// have greater IDs.
type View struct {
	ID     ID     `json:"id"`
	Config Config `json:"config"`
}

// GetVersionedRulesConfig specializes the view to just the rules config.
func (v View) GetVersionedRulesConfig() VersionedRulesConfig {
	return VersionedRulesConfig{
		ID:     v.ID,
		Config: v.Config.RulesFiles,
	}
}

// RulesConfig are the set of rules files for a particular organization.
type RulesConfig map[string]string

// VersionedRulesConfig is a RulesConfig together with a version.
// `data Versioned a = Versioned { id :: ID , config :: a }`
type VersionedRulesConfig struct {
	ID     ID          `json:"id"`
	Config RulesConfig `json:"config"`
}
