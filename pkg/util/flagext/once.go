package flagext

import (
	"flag"
	"time"
)

// IntVarOnce will check to see if a flag has already been registered. If
// so it will set the value to the value already parsed. If not, it will
// register the new flag.
func IntVarOnce(f *flag.FlagSet, p *int, name string, value int, usage string) {
	if fl := f.Lookup(name); fl == nil {
		f.IntVar(p, name, value, usage)
	} else {
		*p = fl.Value.(flag.Getter).Get().(int)
	}
}

// BoolVarOnce will check to see if a flag has already been registered. If
// so it will set the value to the value already parsed. If not, it will
// register the new flag.
func BoolVarOnce(f *flag.FlagSet, p *bool, name string, value bool, usage string) {
	if fl := f.Lookup(name); fl == nil {
		f.BoolVar(p, name, value, usage)
	} else {
		*p = fl.Value.(flag.Getter).Get().(bool)
	}
}

// StringVarOnce will check to see if a flag has already been registered. If
// so it will set the value to the value already parsed. If not, it will
// register the new flag.
func StringVarOnce(f *flag.FlagSet, p *string, name string, value string, usage string) {
	if fl := f.Lookup(name); fl == nil {
		f.StringVar(p, name, value, usage)
	} else {
		*p = fl.Value.(flag.Getter).Get().(string)
	}
}

// DurationVarOnce will check to see if a flag has already been registered. If
// so it will set the value to the value already parsed. If not, it will
// register the new flag.
func DurationVarOnce(f *flag.FlagSet, p *time.Duration, name string, value time.Duration, usage string) {
	if fl := f.Lookup(name); fl == nil {
		f.DurationVar(p, name, value, usage)
	} else {
		*p = fl.Value.(flag.Getter).Get().(time.Duration)
	}
}
