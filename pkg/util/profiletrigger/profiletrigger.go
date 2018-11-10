package profiletrigger

import (
	"flag"
	"time"

	"github.com/Dieterbe/profiletrigger/heap"
)

// Config for profiletrigger.
type Config struct {
	enabled bool
	heap.Config
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.enabled, "profile-trigger.enabled", false, "Enable profile trigger.")
	f.StringVar(&c.Path, "profile-trigger.dir", "./", "Directory to save trigger profiles to.")
	f.IntVar(&c.RSSThreshold, "profile-trigger.threshold", 10*1024*1024*1024, "Memory consumption at which to trigger profile.")
	f.DurationVar(&c.MinTimeDiff, "profile-trigger.min-time-diff", 5*time.Minute, "Minimum duration between profiles.")
	f.DurationVar(&c.CheckEvery, "profile-trigger.check-duration", 5*time.Second, "Period with which to check memory consumption.")
}

// Run the profile trigger.
func (c *Config) Run() error {
	if !c.enabled {
		return nil
	}

	trigger, err := heap.New(c.Config, nil)
	if err == nil {
		go trigger.Run()
	}

	return err
}
