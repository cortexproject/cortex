// Copyright The OpenTelemetry Authors
<<<<<<< HEAD
// SPDX-License-Identifier: Apache-2.0
=======
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
>>>>>>> 90dc0587b (Initial OTLP ingest support)

package featuregate // import "go.opentelemetry.io/collector/featuregate"

import (
	"flag"
	"strings"

	"go.uber.org/multierr"
)

<<<<<<< HEAD
const (
	featureGatesFlag            = "feature-gates"
	featureGatesFlagDescription = "Comma-delimited list of feature gate identifiers. Prefix with '-' to disable the feature. '+' or no prefix will enable the feature."
)

// RegisterFlagsOption is an option for RegisterFlags.
type RegisterFlagsOption interface {
	private()
}

// RegisterFlags that directly applies feature gate statuses to a Registry.
func (r *Registry) RegisterFlags(flagSet *flag.FlagSet, _ ...RegisterFlagsOption) {
	flagSet.Var(&flagValue{reg: r}, featureGatesFlag, featureGatesFlagDescription)
=======
// NewFlag returns a flag.Value that directly applies feature gate statuses to a Registry.
func NewFlag(reg *Registry) flag.Value {
	return &flagValue{reg: reg}
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// flagValue implements the flag.Value interface and directly applies feature gate statuses to a Registry.
type flagValue struct {
	reg *Registry
}

func (f *flagValue) String() string {
	var ids []string
	f.reg.VisitAll(func(g *Gate) {
		id := g.ID()
		if !g.IsEnabled() {
			id = "-" + id
		}
		ids = append(ids, id)
	})
	return strings.Join(ids, ",")
}

func (f *flagValue) Set(s string) error {
	if s == "" {
		return nil
	}

	var errs error
	ids := strings.Split(s, ",")
	for i := range ids {
		id := ids[i]
		val := true
		switch id[0] {
		case '-':
			id = id[1:]
			val = false
		case '+':
			id = id[1:]
		}
		errs = multierr.Append(errs, f.reg.Set(id, val))
	}
	return errs
}
