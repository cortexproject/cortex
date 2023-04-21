// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package parse

import (
	"fmt"

	"github.com/efficientgo/core/errors"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"
)

var ErrNotSupportedExpr = errors.New("unsupported expression")

func UnsupportedOperationErr(op parser.ItemType) error {
	t := parser.ItemTypeStr[op]
	msg := fmt.Sprintf("operation not supported: %s", t)
	return errors.Wrap(ErrNotSupportedExpr, msg)
}

var ErrNotImplemented = errors.New("expression not implemented")
