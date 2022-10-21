// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoprop // import "go.opentelemetry.io/contrib/propagators/autoprop"

import (
	"errors"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// otelPropagatorsEnvKey is the environment variable name identifying
// propagators to use.
const otelPropagatorsEnvKey = "OTEL_PROPAGATORS"

// NewTextMapPropagator returns a new TextMapPropagator composited by props or
// one defined by the OTEL_PROPAGATORS environment variable. The
// TextMapPropagator defined by OTEL_PROPAGATORS, if set, will take precedence
// to the once composited by props.
//
// The propagators supported with the OTEL_PROPAGATORS environment variable by
// default are: tracecontext, baggage, b3, b3multi, jaeger, xray, ottrace, and
// none. Each of these values, and their combination, are supported in
// conformance with the OpenTelemetry specification. See
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md#general-sdk-configuration
// for more information.
//
// The supported environment variable propagators can be extended to include
// custom 3rd-party TextMapPropagator. See the RegisterTextMapPropagator
// function for more information.
//
// If OTEL_PROPAGATORS is not defined and props is no provided, the returned
// TextMapPropagator will be a composite of the TraceContext and Baggage
// propagators.
func NewTextMapPropagator(props ...propagation.TextMapPropagator) propagation.TextMapPropagator {
	// Environment variable defined propagator has precedence over arguments.
	envProp, err := parseEnv()
	if err != nil {
		// Communicate to the user their supplied value will not be used.
		otel.Handle(err)
	}
	if envProp != nil {
		return envProp
	}

	switch len(props) {
	case 0:
		// Default to TraceContext and Baggage.
		return propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		)
	case 1:
		// Do not add overhead with a composite propagator wrapping a single
		// propagator, return it directly.
		return props[0]
	default:
		return propagation.NewCompositeTextMapPropagator(props...)
	}
}

// errUnknownPropagator is returned when an unknown propagator name is used in
// the OTEL_PROPAGATORS environment variable.
var errUnknownPropagator = errors.New("unknown propagator")

// parseEnv returns the composite TextMapPropagators defined by the
// OTEL_PROPAGATORS environment variable. A nil TextMapPropagator is returned
// if no propagator is defined for the environment variable. A no-op
// TextMapPropagator will be returned if "none" is defined anywhere in the
// environment variable.
func parseEnv() (propagation.TextMapPropagator, error) {
	propStrs, defined := os.LookupEnv(otelPropagatorsEnvKey)
	if !defined {
		return nil, nil
	}
	return TextMapPropagator(strings.Split(propStrs, ",")...)
}
