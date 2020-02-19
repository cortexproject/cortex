package cortex

import (
	"fmt"
	"net/http"
)

func (t *Cortex) servicesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Type", "text/plain")

	for mod := moduleName(0); mod < All; mod++ {
		s := t.serviceMap[mod]
		if s != nil {
			fmt.Fprintf(w, "%v => %v\n", mod, s.State())
		}
	}
}
