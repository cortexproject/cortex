package util

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"google.golang.org/grpc/metadata"
)

// ipAddressesKey is key for the GRPC metadata where the IP addresses are stored
const ipAddressesKey = "ipaddresseskey"

// extractHost returns the Host IP address without any port information
func extractHost(address string) string {
	hostIP := net.ParseIP(address)
	if hostIP != nil {
		return hostIP.String()
	}
	var err error
	hostStr, _, err := net.SplitHostPort(address)
	if err != nil {
		// Invalid IP address, just return it so it shows up in the logs
		return address
	}
	return hostStr
}

// GetSource extracts the X-FORWARDED-FOR header from the given HTTP request
// and returns a string with it and the remote address
func GetSource(req *http.Request) string {
	fwd := req.Header.Get("X-FORWARDED-FOR")
	if fwd == "" {
		if req.RemoteAddr == "" {
			// No X-FORWARDED-FOR header and no RemoteAddr set so we want to return an empty string
			// Might as well just use the empty string set in req.RemoteAddr
			return req.RemoteAddr
		}
		return extractHost(req.RemoteAddr)
	}
	// If RemoteAddr is empty just return the header
	if req.RemoteAddr == "" {
		return fwd
	}
	// If both a header and RemoteAddr are present return them both, stripping off any port info from the RemoteAddr
	return fmt.Sprintf("%v, %v", fwd, extractHost(req.RemoteAddr))
}

// GetSourceFromOutgoingCtx extracts the source field from the GRPC context
func GetSourceFromOutgoingCtx(ctx context.Context) string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ""
	}
	ipAddresses, ok := md[ipAddressesKey]
	if !ok {
		return ""
	}
	return ipAddresses[0]
}

// GetSourceFromIncomingCtx extracts the source field from the GRPC context
func GetSourceFromIncomingCtx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	ipAddresses, ok := md[ipAddressesKey]
	if !ok {
		return ""
	}
	return ipAddresses[0]
}

// AddSourceToOutgoingContext adds the given source to the GRPC context
func AddSourceToOutgoingContext(ctx context.Context, source string) context.Context {
	if source != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, ipAddressesKey, source)
	}
	return ctx
}

// AddSourceToIncomingContext adds the given source to the GRPC context
func AddSourceToIncomingContext(ctx context.Context, source string) context.Context {
	if source != "" {
		md := metadata.Pairs(ipAddressesKey, source)
		ctx = metadata.NewIncomingContext(ctx, md)
	}
	return ctx
}
