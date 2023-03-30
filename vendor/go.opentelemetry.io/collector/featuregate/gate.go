// Copyright The OpenTelemetry Authors
<<<<<<< HEAD
// SPDX-License-Identifier: Apache-2.0

package featuregate // import "go.opentelemetry.io/collector/featuregate"

import (
	"fmt"
	"sync/atomic"

	"github.com/hashicorp/go-version"
)
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

package featuregate // import "go.opentelemetry.io/collector/featuregate"

import "sync/atomic"
>>>>>>> 90dc0587b (Initial OTLP ingest support)

// Gate is an immutable object that is owned by the Registry and represents an individual feature that
// may be enabled or disabled based on the lifecycle state of the feature and CLI flags specified by the user.
type Gate struct {
<<<<<<< HEAD
	id           string
	description  string
	referenceURL string
	fromVersion  *version.Version
	toVersion    *version.Version
	stage        Stage
	enabled      *atomic.Bool
=======
	id             string
	description    string
	referenceURL   string
	removalVersion string
	stage          Stage
	enabled        *atomic.Bool
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}

// ID returns the id of the Gate.
func (g *Gate) ID() string {
	return g.id
}

// IsEnabled returns true if the feature described by the Gate is enabled.
func (g *Gate) IsEnabled() bool {
	return g.enabled.Load()
}

// Description returns the description for the Gate.
func (g *Gate) Description() string {
	return g.description
}

// Stage returns the Gate's lifecycle stage.
func (g *Gate) Stage() Stage {
	return g.stage
}

// ReferenceURL returns the URL to the contextual information about the Gate.
func (g *Gate) ReferenceURL() string {
	return g.referenceURL
}

<<<<<<< HEAD
// FromVersion returns the version information when the Gate's was added.
func (g *Gate) FromVersion() string {
	return fmt.Sprintf("v%s", g.fromVersion)
}

// ToVersion returns the version information when Gate's in StageStable.
func (g *Gate) ToVersion() string {
	return fmt.Sprintf("v%s", g.toVersion)
=======
// RemovalVersion returns the removal version information for Gate's in StageStable.
func (g *Gate) RemovalVersion() string {
	return g.removalVersion
>>>>>>> 90dc0587b (Initial OTLP ingest support)
}
