package local

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	promRules "github.com/prometheus/prometheus/rules"

	cortexparser "github.com/cortexproject/cortex/pkg/parser"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// FileLoader is a promRules.GroupLoader that parses rule files with the Cortex
// PromQL parser. Prometheus' own rules.FileLoader has no exported way to set a
// parser, and its zero value panics when it parses an expression.
type FileLoader struct {
	parser parser.Parser
}

var _ promRules.GroupLoader = FileLoader{}

// NewFileLoader returns a FileLoader using the Cortex engine parser.
func NewFileLoader() FileLoader {
	return FileLoader{parser: cortexparser.NewEngineParser()}
}

func (l FileLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	return rulefmt.ParseFile(identifier, ignoreUnknownFields, nameValidationScheme, l.parser, util_log.SLogger)
}

func (l FileLoader) Parse(query string) (parser.Expr, error) {
	return l.parser.ParseExpr(query)
}
