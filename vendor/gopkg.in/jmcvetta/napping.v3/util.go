// Copyright (c) 2012-2013 Jason McVetta.  This is Free Software, released
// under the terms of the GPL v3.  See http://www.gnu.org/copyleft/gpl.html for
// details.  Resist intellectual serfdom - the ownership of ideas is akin to
// slavery.

package napping

import (
	"encoding/json"
)

// pretty pretty-prints an interface using the JSON marshaler
func pretty(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "\t")
	return string(b)
}
