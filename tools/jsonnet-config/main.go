package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/cortexproject/cortex/pkg/config"
	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/jsonnet-libs/k8s/pkg/builder"
)

func toJsonType(b *config.ConfigEntry) builder.Type {
	switch ft := b.FieldType; ft {
	case "string", "duration", "url", "time":
		return builder.String(b.Name, b.FieldDefault)
	case "boolean":
		value, err := strconv.ParseBool(b.FieldDefault)
		if err != nil {
			panic(fmt.Sprintf("unable to parse %s value as bool %s", b.Name, b.FieldDefault))
		}
		return builder.Bool(b.Name, value)
	case "int":
		value, err := strconv.ParseInt(b.FieldDefault, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("unable to parse %s value as int %s", b.Name, b.FieldDefault))
		}
		return builder.Int(b.Name, int(value))
	case "float":
		value, err := strconv.ParseFloat(b.FieldDefault, 64)
		if err != nil {
			panic(fmt.Sprintf("unable to parse %s value as float %s", b.Name, b.FieldDefault))
		}
		return builder.Float(b.Name, value)
	case "list of string", "relabel_config...", "list of duration":
		re := regexp.MustCompile(`[^\[\]\s,]+`)
		vals := re.FindAllString(b.FieldDefault, -1)
		if vals == nil {
			return builder.List(b.Name)
		}
		builderList := []builder.Type{}
		for _, s := range vals {
			builderList = append(builderList, builder.String("", s))
		}
		return builder.List(b.Name, builderList...)
	case "map of string to string":
		// Only the `deletes.table.tags` flag uses this format and the default is an empty object so we just use an empty object
		return builder.Object(b.Name)
	default:
		panic(fmt.Sprintf("unsupported data type %s", b.FieldType))
	}
}

func reusable(blocks []*config.ConfigBlock) []builder.Type {
	entries := []builder.Type{}
	seen := map[string]bool{}
	for _, b := range blocks {
		if b.Name != "" {
			if _, ok := seen[b.Name]; !ok {
				seen[b.Name] = true
				subEntries := []builder.Type{}
				for _, e := range b.Entries {
					subEntries = append(subEntries, fromConfigEntry(e))
				}
				entries = append(entries, builder.Hidden(builder.Object(b.Name, builder.Func("new", nil, builder.Object("", subEntries...)))))
			}
		} else {
			entries = append(entries, builder.Object("TODO"))
		}
	}
	return entries
}

func fromConfigEntry(e *config.ConfigEntry) builder.Type {
	subEntries := []builder.Type{}
	if e.Kind == "block" {
		for _, e := range e.Block.Entries {
			subEntries = append(subEntries, fromConfigEntry(e))
		}
		return builder.Object(e.Name, subEntries...)
	}
	return toJsonType(e)
}

func toFlag(e *config.ConfigEntry) builder.Type {
	subEntries := []builder.Type{}
	if e.Kind == "block" {
		for _, e := range e.Block.Entries {
			subEntries = append(subEntries, toFlag(e))
		}
		return builder.Object(e.Name, subEntries...)
	}
	return builder.String(e.Name, e.FieldFlag)
}

func flags(block *config.ConfigBlock) builder.Type {
	entries := []builder.Type{}
	for _, e := range block.Entries {
		entries = append(entries, toFlag(e))
	}
	return builder.Hidden(builder.Object("flags", entries...))
}

func defaults(block *config.ConfigBlock) builder.Type {
	entries := []builder.Type{}
	for _, e := range block.Entries {
		entries = append(entries, fromConfigEntry(e))
	}
	return builder.Hidden(builder.Object("defaults", entries...))
}

func toType(e *config.ConfigEntry) builder.Type {
	subEntries := []builder.Type{}
	if e.Kind == "block" {
		for _, e := range e.Block.Entries {
			subEntries = append(subEntries, toType(e))
		}
		return builder.Object(e.Name, subEntries...)
	}
	return builder.String(e.Name, e.FieldType)
}

func types(block *config.ConfigBlock) builder.Type {
	entries := []builder.Type{}
	for _, e := range block.Entries {
		entries = append(entries, toType(e))
	}
	return builder.Hidden(builder.Object("types", entries...))
}

func main() {
	cfg := &cortex.Config{}
	fs := config.ParseFlags(cfg)

	// Parse the config, mapping each config field with the related CLI flag.
	blocks, err := config.ParseConfig(nil, cfg, fs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while generating the library: %s\n", err.Error())
		os.Exit(1)
	}

	fields := append(reusable(blocks[1:]), flags(blocks[0]), defaults(blocks[0]), types(blocks[0]))
	fmt.Println(builder.Object("", fields...))
}
