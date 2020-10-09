package main

import (
	"flag"
	"log"
	"net"
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
	flag.StringVar(&cfg.LocalAddress, "local-address", ":8080", "Local address to listen on (host:port or :port).")
	flag.StringVar(&cfg.RemoteURL, "remote-address", "", "URL of target to forward requests to to (eg. http://domain.com:80).")
	flag.StringVar(&cfg.TenantID, "tenant-id", "", "Tenant ID to inject to proxied requests.")
	flag.Parse()

	// Parse remote URL.
	if cfg.RemoteURL == "" {
		log.Fatalln("No -remote-address specified.")
	}

	remoteURL, err := url.Parse(cfg.RemoteURL)
	if err != nil {
		log.Fatalf("Unable to parse remote address. Error: %s.", err.Error())
	}
	log.Println("Forwarding to", remoteURL)

	ln, err := net.Listen("tcp", cfg.LocalAddress)
	if err != nil {
		log.Fatal(err)
	}

	s := &http.Server{
		Addr:           cfg.LocalAddress,
		Handler:        injectAuthHeader(cfg.TenantID, httputil.NewSingleHostReverseProxy(remoteURL)),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Println("Listening on", ln.Addr())
	log.Fatal(s.Serve(ln))
}

func injectAuthHeader(tenantID string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Scope-OrgID", tenantID)
		h.ServeHTTP(w, r)
	})
}
