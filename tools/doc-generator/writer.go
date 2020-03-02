package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	wordwrap "github.com/mitchellh/go-wordwrap"
)

type specWriter struct {
	out strings.Builder
}

func (w *specWriter) writeConfigBlock(b *configBlock, indent int) {
	if len(b.entries) == 0 {
		return
	}

	for i, entry := range b.entries {
		// Add a new line to separate from the previous entry
		if i > 0 {
			w.out.WriteString("\n")
		}

		w.writeConfigEntry(entry, indent)
	}
}

func (w *specWriter) writeConfigEntry(e *configEntry, indent int) {
	if e.kind == "block" {
		// If the block is a root block it will have its dedicated section in the doc,
		// so here we've just to write down the reference without re-iterating on it.
		if e.root {
			// Description
			w.writeComment(e.blockDesc, indent)
			if e.block.flagsPrefix != "" {
				w.writeComment(fmt.Sprintf("The CLI flags prefix for this block config is: %s", e.block.flagsPrefix), indent)
			}

			// Block reference without entries, because it's a root block
			w.out.WriteString(pad(indent) + "[" + e.name + ": <" + e.block.name + ">]\n")
		} else {
			// Description
			w.writeComment(e.blockDesc, indent)

			// Name
			w.out.WriteString(pad(indent) + e.name + ":\n")

			// Entries
			w.writeConfigBlock(e.block, indent+tabWidth)
		}
	}

	if e.kind == "field" {
		// Description
		w.writeComment(e.fieldDesc, indent)
		w.writeFlag(e.fieldFlag, indent)

		// Specification
		fieldDefault := e.fieldDefault
		if e.fieldType == "string" {
			fieldDefault = strconv.Quote(fieldDefault)
		}

		if e.required {
			w.out.WriteString(pad(indent) + e.name + ": <" + e.fieldType + "> | default = " + fieldDefault + "\n")
		} else {
			w.out.WriteString(pad(indent) + "[" + e.name + ": <" + e.fieldType + "> | default = " + fieldDefault + "]\n")
		}
	}
}

func (w *specWriter) writeFlag(name string, indent int) {
	if name == "" {
		return
	}

	w.out.WriteString(pad(indent) + "# CLI flag: -" + name + "\n")
}

func (w *specWriter) writeComment(comment string, indent int) {
	if comment == "" {
		return
	}

	wrapped := strings.TrimSpace(wordwrap.WrapString(comment, uint(maxLineWidth-indent-2)))
	lines := strings.Split(wrapped, "\n")

	for _, line := range lines {
		w.out.WriteString(pad(indent) + "# " + line + "\n")
	}
}

func (w *specWriter) string() string {
	return strings.TrimSpace(w.out.String())
}

type markdownWriter struct {
	out strings.Builder
}

func (w *markdownWriter) writeConfigDoc(blocks []*configBlock) {
	// Deduplicate root blocks.
	uniqueBlocks := map[string]*configBlock{}
	for _, block := range blocks {
		uniqueBlocks[block.name] = block
	}

	// Generate the markdown, honoring the root blocks order.
	if topBlock, ok := uniqueBlocks[""]; ok {
		w.writeConfigBlock(topBlock)
	}

	for _, rootBlock := range rootBlocks {
		if block, ok := uniqueBlocks[rootBlock.name]; ok {
			w.writeConfigBlock(block)
		}
	}
}

func (w *markdownWriter) writeConfigBlock(block *configBlock) {
	// Title
	if block.name != "" {
		w.out.WriteString("### `" + block.name + "`\n")
		w.out.WriteString("\n")
	}

	// Description
	if block.desc != "" {
		desc := block.desc

		// Wrap the config block name with backticks
		if block.name != "" {
			desc = regexp.MustCompile(regexp.QuoteMeta(block.name)).ReplaceAllStringFunc(desc, func(input string) string {
				return "`" + input + "`"
			})
		}

		w.out.WriteString(desc + "\n")
		w.out.WriteString("\n")
	}

	// Config specs
	spec := &specWriter{}
	spec.writeConfigBlock(block, 0)

	w.out.WriteString("```yaml\n")
	w.out.WriteString(spec.string() + "\n")
	w.out.WriteString("```\n")
	w.out.WriteString("\n")
}

func (w *markdownWriter) string() string {
	return strings.TrimSpace(w.out.String())
}

func pad(length int) string {
	return strings.Repeat(" ", length)
}
