package main

import (
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	yamlFieldNameParser   = regexp.MustCompile("^[^,]+")
	yamlFieldInlineParser = regexp.MustCompile("^[^,]*,inline$")
)

type configBlock struct {
	name          string
	desc          string
	entries       []*configEntry
	flagsPrefix   string
	flagsPrefixes []string
}

func (b *configBlock) Add(entry *configEntry) {
	b.entries = append(b.entries, entry)
}

type configEntry struct {
	kind     string
	name     string
	required bool

	// In case the kind is "block"
	block     *configBlock
	blockDesc string
	root      bool

	// In case the kind is "field"
	fieldFlag    string
	fieldDesc    string
	fieldType    string
	fieldDefault string
}

type rootBlock struct {
	name       string
	desc       string
	structType reflect.Type
}

func parseFlags(cfg flagext.Registerer) map[uintptr]*flag.Flag {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	cfg.RegisterFlags(fs)

	flags := map[uintptr]*flag.Flag{}
	fs.VisitAll(func(f *flag.Flag) {
		// Skip deprecated flags
		if f.Value.String() == "deprecated" {
			return
		}

		ptr := reflect.ValueOf(f.Value).Pointer()
		flags[ptr] = f
	})

	return flags
}

func parseConfig(block *configBlock, cfg interface{}, flags map[uintptr]*flag.Flag) ([]*configBlock, error) {
	blocks := []*configBlock{}

	// If the input block is nil it means we're generating the doc for the top-level block
	if block == nil {
		block = &configBlock{}
		blocks = append(blocks, block)
	}

	// The input config is expected to be addressable.
	if reflect.TypeOf(cfg).Kind() != reflect.Ptr {
		t := reflect.TypeOf(cfg)
		return nil, fmt.Errorf("%s is a %s while a %s is expected", t, t.Kind(), reflect.Ptr)
	}

	// The input config is expected to be a pointer to struct.
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is a %s while a %s is expected", v, v.Kind(), reflect.Struct)
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.FieldByIndex(field.Index)

		// Skip fields explicitly marked as "hidden" in the doc
		if isFieldHidden(field) {
			continue
		}

		// Skip fields not exported via yaml (unless they're inline)
		fieldName := getFieldName(field)
		if fieldName == "" && !isFieldInline(field) {
			continue
		}

		// Skip field types which are non configurable
		if field.Type.Kind() == reflect.Func {
			continue
		}

		// Skip deprecated fields we're still keeping for backward compatibility
		// reasons (by convention we prefix them by UnusedFlag)
		if strings.HasPrefix(field.Name, "UnusedFlag") {
			continue
		}

		// Handle custom fields in vendored libs upon which we have no control.
		fieldEntry, err := getCustomFieldEntry(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}
		if fieldEntry != nil {
			block.Add(fieldEntry)
			continue
		}

		// Recursively re-iterate if it's a struct
		if field.Type.Kind() == reflect.Struct {
			// Check whether the sub-block is a root config block
			rootName, rootDesc, isRoot := isRootBlock(field.Type)

			// Since we're going to recursively iterate, we need to create a new sub
			// block and pass it to the doc generation function.
			var subBlock *configBlock

			if !isFieldInline(field) {
				var blockName string
				var blockDesc string

				if isRoot {
					blockName = rootName
					blockDesc = rootDesc
				} else {
					blockName = fieldName
					blockDesc = getFieldDescription(field, "")
				}

				subBlock = &configBlock{
					name: blockName,
					desc: blockDesc,
				}

				block.Add(&configEntry{
					kind:      "block",
					name:      fieldName,
					required:  isFieldRequired(field),
					block:     subBlock,
					blockDesc: blockDesc,
					root:      isRoot,
				})

				if isRoot {
					blocks = append(blocks, subBlock)
				}
			} else {
				subBlock = block
			}

			// Recursively generate the doc for the sub-block
			otherBlocks, err := parseConfig(subBlock, fieldValue.Addr().Interface(), flags)
			if err != nil {
				return nil, err
			}

			blocks = append(blocks, otherBlocks...)
			continue
		}

		fieldType, err := getFieldType(field.Type)
		if err != nil {
			return nil, errors.Wrapf(err, "config=%s.%s", t.PkgPath(), t.Name())
		}

		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, errors.Wrapf(err, "config=%s.%s", t.PkgPath(), t.Name())
		}
		if fieldFlag == nil {
			block.Add(&configEntry{
				kind:      "field",
				name:      fieldName,
				required:  isFieldRequired(field),
				fieldDesc: getFieldDescription(field, ""),
				fieldType: fieldType,
			})
			continue
		}

		block.Add(&configEntry{
			kind:         "field",
			name:         fieldName,
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    getFieldDescription(field, fieldFlag.Usage),
			fieldType:    fieldType,
			fieldDefault: fieldFlag.DefValue,
		})
	}

	return blocks, nil
}

func getFieldName(field reflect.StructField) string {
	name := field.Name
	tag := field.Tag.Get("yaml")

	// If the tag is not specified, then an exported field can be
	// configured via the field name (lowercase), while an unexported
	// field can't be configured.
	if tag == "" {
		if unicode.IsLower(rune(name[0])) {
			return ""
		}

		return strings.ToLower(name)
	}

	// Parse the field name
	fieldName := yamlFieldNameParser.FindString(tag)
	if fieldName == "-" {
		return ""
	}

	return fieldName
}

func getFieldType(t reflect.Type) (string, error) {
	// Handle custom data types used in the config
	switch t.String() {
	case "*url.URL":
		return "url", nil
	case "time.Duration":
		return "duration", nil
	case "cortex.moduleName":
		return "string", nil
	case "flagext.StringSliceCSV":
		return "string", nil
	case "flagext.CIDRSliceCSV":
		return "string", nil
	case "[]*relabel.Config":
		return "relabel_config...", nil
	}

	// Fallback to auto-detection of built-in data types
	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil

	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return "int", nil

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return "float", nil

	case reflect.String:
		return "string", nil

	case reflect.Slice:
		// Get the type of elements
		elemType, err := getFieldType(t.Elem())
		if err != nil {
			return "", err
		}

		return "list of " + elemType, nil

	case reflect.Map:
		return fmt.Sprintf("map of %s to %s", t.Key(), t.Elem().String()), nil

	default:
		return "", fmt.Errorf("unsupported data type %s", t.Kind())
	}
}

func getFieldFlag(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) (*flag.Flag, error) {
	if isAbsentInCLI(field) {
		return nil, nil
	}
	fieldPtr := fieldValue.Addr().Pointer()
	fieldFlag, ok := flags[fieldPtr]
	if !ok {
		return nil, fmt.Errorf("unable to find CLI flag for '%s' config entry", field.Name)
	}

	return fieldFlag, nil
}

func getCustomFieldEntry(field reflect.StructField, fieldValue reflect.Value, flags map[uintptr]*flag.Flag) (*configEntry, error) {
	if field.Type == reflect.TypeOf(logging.Level{}) || field.Type == reflect.TypeOf(logging.Format{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    fieldFlag.Usage,
			fieldType:    "string",
			fieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.URLValue{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    fieldFlag.Usage,
			fieldType:    "url",
			fieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.Secret{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    fieldFlag.Usage,
			fieldType:    "string",
			fieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(model.Duration(0)) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    fieldFlag.Usage,
			fieldType:    "duration",
			fieldDefault: fieldFlag.DefValue,
		}, nil
	}
	if field.Type == reflect.TypeOf(flagext.Time{}) {
		fieldFlag, err := getFieldFlag(field, fieldValue, flags)
		if err != nil {
			return nil, err
		}

		return &configEntry{
			kind:         "field",
			name:         getFieldName(field),
			required:     isFieldRequired(field),
			fieldFlag:    fieldFlag.Name,
			fieldDesc:    fieldFlag.Usage,
			fieldType:    "time",
			fieldDefault: fieldFlag.DefValue,
		}, nil
	}

	return nil, nil
}

func isFieldHidden(f reflect.StructField) bool {
	return getDocTagFlag(f, "hidden")
}

func isAbsentInCLI(f reflect.StructField) bool {
	return getDocTagFlag(f, "nocli")
}

func isFieldRequired(f reflect.StructField) bool {
	return getDocTagFlag(f, "required")
}

func isFieldInline(f reflect.StructField) bool {
	return yamlFieldInlineParser.MatchString(f.Tag.Get("yaml"))
}

func getFieldDescription(f reflect.StructField, fallback string) string {
	if desc := getDocTagValue(f, "description"); desc != "" {
		return desc
	}

	return fallback
}

func isRootBlock(t reflect.Type) (string, string, bool) {
	for _, rootBlock := range rootBlocks {
		if t == rootBlock.structType {
			return rootBlock.name, rootBlock.desc, true
		}
	}

	return "", "", false
}

func getDocTagFlag(f reflect.StructField, name string) bool {
	cfg := parseDocTag(f)
	_, ok := cfg[name]
	return ok
}

func getDocTagValue(f reflect.StructField, name string) string {
	cfg := parseDocTag(f)
	return cfg[name]
}

func parseDocTag(f reflect.StructField) map[string]string {
	cfg := map[string]string{}
	tag := f.Tag.Get("doc")

	if tag == "" {
		return cfg
	}

	for _, entry := range strings.Split(tag, "|") {
		parts := strings.SplitN(entry, "=", 2)

		switch len(parts) {
		case 1:
			cfg[parts[0]] = ""
		case 2:
			cfg[parts[0]] = parts[1]
		}
	}

	return cfg
}
