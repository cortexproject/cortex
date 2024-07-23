// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package autoprop provides an OpenTelemetry TextMapPropagator creation
// function. The OpenTelemetry specification states that the default
// TextMapPropagator needs to be a no-operation implementation. The
// opentelemetry-go project adheres to this requirement. However, for systems
// that perform propagation this default is not ideal. This package provides a
// TextMapPropagator with useful defaults (a combined TraceContext and Baggage
// TextMapPropagator), and supports environment overrides using the
// OTEL_PROPAGATORS environment variable.
package autoprop // import "go.opentelemetry.io/contrib/propagators/autoprop"
