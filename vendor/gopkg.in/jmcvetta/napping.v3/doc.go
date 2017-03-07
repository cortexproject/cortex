// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released
// under the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for
// details.  Resist intellectual serfdom - the ownership of ideas is akin to
// slavery.

/*
Package napping is a client library for interacting with RESTful APIs.

Example:

	type Foo struct {
		Bar string
	}
	type Spam struct {
		Eggs int
	}
	payload := Foo{
		Bar: "baz",
	}
	result := Spam{}
	url := "http://foo.com/bar"
	resp, err := napping.Post(url, &payload, &result, nil)
	if err != nil {
		panic(err)
	}
	if resp.Status() == 200 {
		println(result.Eggs)
	}

See the "examples" folder for a more complete example.
*/
package napping
