package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type Config struct {
	LocalAddress string
	RemoteURL    string
	TenantID     string
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.LocalAddress, "local-address", "", "Local address to listen to (eg. localhost:8080).")
	flag.StringVar(&cfg.RemoteURL, "remote-address", "", "Remote address to proxy to (eg. http://domain.com:80).")
	flag.StringVar(&cfg.TenantID, "tenant-id", "", "Tenant ID to inject to proxied requests.")
	flag.Parse()

	// Parse remote URL.
	remoteURL, err := url.Parse(cfg.RemoteURL)
	if err != nil {
		log.Fatalf("Unable to parse remote address. Error: %s.", err.Error())
	}

	s := &http.Server{
		Addr:           cfg.LocalAddress,
		Handler:        injectAuthHeader(cfg.TenantID, httputil.NewSingleHostReverseProxy(remoteURL)),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())
}

func injectAuthHeader(tenantID string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Scope-OrgID", tenantID)
		h.ServeHTTP(w, r)
	})
}
