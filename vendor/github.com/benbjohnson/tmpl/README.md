# tmpl

This program is a command line interface to Go's `text/template` library. It
can be used by passing in a set of JSON-encoded data and a list of template
paths ending in a `.tmpl` extension. The templates are processed and their
results are saved to the filename with the `.tmpl` extension removed.


## Getting Started

To install `tmpl`, simply run:

```sh
$ go get github.com/benbjohnson/tmpl
```

Then run `tmpl` against your desired templates:

```sh
$ tmpl -data '["foo","bar","baz"]' a.go.tmpl b.go.tmpl
```

You will now have templates generated at `a.go` and `b.go`.


### Template data files

Once your data set gets larger, it may be useful to move it to its own file
instead of specifying it on the command line. To use a data file instead
of raw JSON on the command line, use the `-data` argument but prefix your
path with an `@` symbol.

```sh
$ echo '["foo","bar","baz"]' > tmpldata
$ tmpl -data=@tmpldata my.tmpl
```
