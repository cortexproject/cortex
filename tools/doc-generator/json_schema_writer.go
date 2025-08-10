package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type JSONSchemaWriter struct {
	out io.Writer
}

func NewJSONSchemaWriter(out io.Writer) *JSONSchemaWriter {
	return &JSONSchemaWriter{out: out}
}

func (w *JSONSchemaWriter) WriteSchema(blocks []*configBlock) error {
	schema := w.generateJSONSchema(blocks)

	encoder := json.NewEncoder(w.out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(schema)
}

func (w *JSONSchemaWriter) generateJSONSchema(blocks []*configBlock) map[string]interface{} {
	schema := map[string]interface{}{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         "https://raw.githubusercontent.com/cortexproject/cortex/master/schemas/cortex-config-schema.json",
		"title":       "Cortex Configuration Schema",
		"description": "JSON Schema for Cortex configuration file",
		"type":        "object",
		"properties":  map[string]interface{}{},
		"definitions": map[string]interface{}{},
	}

	properties := schema["properties"].(map[string]interface{})
	definitions := schema["definitions"].(map[string]interface{})

	// Process each config block
	for _, block := range blocks {
		if block.name == "" {
			// This is the root block, process its entries as top-level properties
			w.processBlockEntries(block, properties, definitions)
		} else {
			// This is a named block, add it to definitions
			definitions[block.name] = w.generateBlockSchema(block)
		}
	}

	return schema
}

func (w *JSONSchemaWriter) processBlockEntries(block *configBlock, properties map[string]interface{}, definitions map[string]interface{}) {
	for _, entry := range block.entries {
		switch entry.kind {
		case "field":
			properties[entry.name] = w.generateFieldSchema(entry)
		case "block":
			if entry.root {
				// Root blocks are referenced via $ref
				properties[entry.name] = map[string]interface{}{
					"$ref": fmt.Sprintf("#/definitions/%s", entry.block.name),
				}
				// Add the block to definitions if not already there
				if _, exists := definitions[entry.block.name]; !exists {
					definitions[entry.block.name] = w.generateBlockSchema(entry.block)
				}
			} else {
				// Inline blocks are embedded directly
				properties[entry.name] = w.generateBlockSchema(entry.block)
			}
		}
	}
}

func (w *JSONSchemaWriter) generateBlockSchema(block *configBlock) map[string]interface{} {
	obj := map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
	}

	if block.desc != "" {
		obj["description"] = block.desc
	}

	properties := obj["properties"].(map[string]interface{})

	for _, entry := range block.entries {
		switch entry.kind {
		case "field":
			properties[entry.name] = w.generateFieldSchema(entry)
		case "block":
			if entry.root {
				// Reference to another root block
				properties[entry.name] = map[string]interface{}{
					"$ref": fmt.Sprintf("#/definitions/%s", entry.block.name),
				}
			} else {
				// Inline nested block
				properties[entry.name] = w.generateBlockSchema(entry.block)
			}
		}
	}

	return obj
}

func (w *JSONSchemaWriter) generateFieldSchema(entry *configEntry) map[string]interface{} {
	prop := map[string]interface{}{
		"type": w.getJSONType(entry.fieldType),
	}

	// Add description
	if entry.fieldDesc != "" {
		prop["description"] = entry.fieldDesc
	}

	// Add default value
	if entry.fieldDefault != "" {
		prop["default"] = w.parseDefaultValue(entry.fieldDefault, entry.fieldType)
	}

	// Add CLI flag information
	if entry.fieldFlag != "" {
		prop["x-cli-flag"] = entry.fieldFlag
	}

	// Add format hints based on type
	switch entry.fieldType {
	case "duration":
		prop["x-format"] = "duration"
		prop["type"] = "string"
	case "url":
		prop["format"] = "uri"
		prop["type"] = "string"
	case "time":
		prop["format"] = "date-time"
		prop["type"] = "string"
	}

	// Handle list types
	if strings.HasPrefix(entry.fieldType, "list of ") {
		prop["type"] = "array"
		itemType := strings.TrimPrefix(entry.fieldType, "list of ")
		prop["items"] = map[string]interface{}{
			"type": w.getJSONType(itemType),
		}
	}

	// Handle map types
	if strings.HasPrefix(entry.fieldType, "map of ") {
		prop["type"] = "object"
		prop["additionalProperties"] = true
	}

	// Mark required fields
	if entry.required {
		prop["x-required"] = true
	}

	return prop
}

func (w *JSONSchemaWriter) getJSONType(goType string) string {
	switch goType {
	case "string":
		return "string"
	case "int", "float":
		return "number"
	case "boolean":
		return "boolean"
	case "duration", "url", "time":
		return "string"
	default:
		// Handle complex types
		if strings.HasPrefix(goType, "list of ") {
			return "array"
		}
		if strings.HasPrefix(goType, "map of ") {
			return "object"
		}
		// Default to string for unknown types
		return "string"
	}
}

func (w *JSONSchemaWriter) parseDefaultValue(defaultStr, goType string) interface{} {
	if defaultStr == "" {
		return nil
	}

	switch goType {
	case "boolean":
		return defaultStr == "true"
	case "int":
		if val, err := parseInt(defaultStr); err == nil {
			return val
		}
		return defaultStr
	case "float":
		if val, err := parseFloat(defaultStr); err == nil {
			return val
		}
		return defaultStr
	default:
		// Handle special cases
		if defaultStr == "[]" {
			return []interface{}{}
		}
		if strings.HasPrefix(defaultStr, "[") && strings.HasSuffix(defaultStr, "]") {
			// Try to parse as JSON array
			var arr []interface{}
			if err := json.Unmarshal([]byte(defaultStr), &arr); err == nil {
				return arr
			}
		}
		return defaultStr
	}
}

// Helper functions for parsing
func parseInt(s string) (int64, error) {
	var result int64
	var err error
	if strings.Contains(s, "e+") || strings.Contains(s, "E+") {
		return 0, fmt.Errorf("scientific notation not supported")
	}
	_, err = fmt.Sscanf(s, "%d", &result)
	return result, err
}

func parseFloat(s string) (float64, error) {
	var result float64
	var err error
	_, err = fmt.Sscanf(s, "%f", &result)
	return result, err
}
