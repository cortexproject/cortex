package ast

//go:generate flatc --go --gen-onefile --go-namespace fbast -o ./internal/fbast/ ./ast.fbs
//go:generate go fmt ./internal/fbast/...
