package fastdialer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/projectdiscovery/fastdialer/fastdialer/ja3/impersonate"
	"github.com/projectdiscovery/fastdialer/fastdialer/utils"
	retryabledns "github.com/projectdiscovery/retryabledns"
	ctxutil "github.com/projectdiscovery/utils/context"
	cryptoutil "github.com/projectdiscovery/utils/crypto"
	"github.com/projectdiscovery/utils/errkit"
	iputil "github.com/projectdiscovery/utils/ip"
	ptrutil "github.com/projectdiscovery/utils/ptr"
	utls "github.com/refraction-networking/utls"
	ztls "github.com/zmap/zcrypto/tls"
)

// this l4dialer interface is used to conditionally pass net.Dialer or its wrapper
type l4dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

var _ l4dialer = &utils.DialWrap{}

type dialOptions struct {
	network             string
	address             string
	shouldUseTLS        bool
	shouldUseZTLS       bool
	tlsconfig           *tls.Config
	ztlsconfig          *ztls.Config
	impersonateStrategy impersonate.Strategy
	impersonateIdentity *impersonate.Identity

	// internally used in dialer_private
	ips      []string
	port     string
	hostname string
}

// connHash returns a hash of the connection
func (d *dialOptions) connHash() string {
	return fmt.Sprintf("%s-%s", d.network, d.address)
}

// logAddress returns the address to be logged in case of error
func (d *dialOptions) logAddress() string {
	logAddress := d.hostname
	if logAddress == "" {
		logAddress = d.ips[0]
	}
	return net.JoinHostPort(logAddress, d.port)
}

func (d *Dialer) dial(ctx context.Context, opts *dialOptions) (conn net.Conn, err error) {
	// add global timeout to context
	ctx, cancel := context.WithTimeoutCause(ctx, d.options.DialerTimeout, ErrDialTimeout)
	defer cancel()

	var hostname, port, fixedIP string
	var IPS []string
	// check if this is present in cache
	dw, err := d.dialCache.GetIFPresent(opts.connHash())
	if err != nil || dw == nil {
		if strings.HasPrefix(opts.address, "[") {
			closeBracketIndex := strings.Index(opts.address, "]")
			if closeBracketIndex == -1 {
				return nil, MalformedIP6Error
			}
			hostname = opts.address[:closeBracketIndex+1]
			if len(opts.address) < closeBracketIndex+2 {
				return nil, NoPortSpecifiedError
			}
			port = opts.address[closeBracketIndex+2:]
		} else {
			addressParts := strings.SplitN(opts.address, ":", 3)
			numberOfParts := len(addressParts)

			if numberOfParts >= 2 {
				// ip|host:port
				hostname = addressParts[0]
				port = addressParts[1]
				// ip|host:port:ip => curl --resolve ip:port:ip
				if numberOfParts > 2 {
					fixedIP = addressParts[2]
				}
				// check if the ip is within the context
				if ctxIP := ctx.Value(IP); ctxIP != nil {
					fixedIP = fmt.Sprint(ctxIP)
				}
			} else {
				// no port => error
				return nil, NoPortSpecifiedError
			}
		}

		// check if data is in cache
		hostname = asAscii(hostname)

		// use fixed ip as first
		if fixedIP != "" {
			IPS = append(IPS, fixedIP)
		} else {
			cacheData, err, _ := d.resolutionsGroup.Do(hostname, func() (interface{}, error) {
				return d.GetDNSData(hostname)
			})

			if cacheData == nil {
				return nil, ResolveHostError
			}
			data := cacheData.(*retryabledns.DNSData)
			if err != nil || len(data.A)+len(data.AAAA) == 0 {
				return nil, NoAddressFoundError
			}
			IPS = append(IPS, append(data.A, data.AAAA...)...)
		}

		filteredIPs := []string{}

		// filter valid/invalid ips
		for _, ip := range IPS {
			// check if we have allow/deny list
			if !d.networkpolicy.Validate(ip) {
				if d.options.OnInvalidTarget != nil {
					d.options.OnInvalidTarget(hostname, ip, port)
				}
				continue
			}
			if d.options.OnBeforeDial != nil {
				d.options.OnBeforeDial(hostname, ip, port)
			}
			// add it as allowed/filter ip
			filteredIPs = append(filteredIPs, ip)
		}

		if len(filteredIPs) == 0 {
			return nil, NoAddressAllowedError
		}

		// update allowed ips
		IPS = filteredIPs
		opts.ips = IPS
		opts.port = port
		opts.hostname = hostname
	}

	var finalDialer l4dialer = d.dialer

	if dw == nil && hostname != "" && len(IPS) > 0 && d.proxyDialer == nil {
		// only cache it if below conditions are met
		// 1. it is already not present
		// 2. it is a domain and not ip
		// 3. it has at least 1 valid ip
		// 4. proxy dialer is not set
		dw, err = utils.NewDialWrap(d.dialer, IPS, opts.network, opts.address, opts.port)
		if err != nil {
			return nil, errkit.Wrap(err, "could not create dialwrap")
		}
		if err = d.dialCache.Set(opts.connHash(), dw); err != nil {
			return nil, errkit.Wrap(err, "could not set dialwrap")
		}
	}
	if dw != nil {
		finalDialer = dw
		// when using dw ip , network , port etc are preset
		// so get any one of them to avoid breaking further logic
		ip, port := dw.Address()
		opts.ips = []string{ip}
		opts.port = port
		// also set hostname by parsing it from address
		hostname, _, _ = net.SplitHostPort(opts.address)
		opts.hostname = hostname
	}

	conn, err = d.dialIPS(ctx, finalDialer, opts)
	if err == nil {
		if conn == nil {
			err = CouldNotConnectError
			return
		}
		if conn.RemoteAddr() == nil {
			return nil, errkit.New("remote address is nil")
		}
		ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		if d.options.WithDialerHistory && d.dialerHistory != nil {
			setErr := d.dialerHistory.Set(hostname, []byte(ip))
			if setErr != nil {
				return nil, setErr
			}
		}
		if d.options.OnDialCallback != nil {
			d.options.OnDialCallback(hostname, ip)
		}
		if d.options.WithTLSData && opts.shouldUseTLS {
			if connTLS, ok := conn.(*tls.Conn); ok {
				var data bytes.Buffer
				connState := connTLS.ConnectionState()
				err := json.NewEncoder(&data).Encode(cryptoutil.TLSGrab(&connState))
				if err != nil {
					return nil, err
				}
				setErr := d.dialerTLSData.Set(hostname, data.Bytes())
				if setErr != nil {
					return nil, setErr
				}
			}
		}
	}
	return
}

func (d *Dialer) dialIPS(ctx context.Context, l4 l4dialer, opts *dialOptions) (conn net.Conn, err error) {
	hostPort := net.JoinHostPort(opts.ips[0], opts.port)

	if opts.shouldUseTLS {
		tlsconfigCopy := opts.tlsconfig.Clone()

		switch {
		case d.options.SNIName != "":
			tlsconfigCopy.ServerName = d.options.SNIName
		case ctx.Value(SniName) != nil:
			sniName := ctx.Value(SniName).(string)
			tlsconfigCopy.ServerName = sniName
		case !iputil.IsIP(opts.hostname):
			tlsconfigCopy.ServerName = opts.hostname
		}

		if opts.impersonateStrategy == impersonate.None {
			l4Conn, err := l4.DialContext(ctx, opts.network, hostPort)
			if err != nil {
				return nil, d.handleDialError(err, opts)
			}
			TlsConn := tls.Client(l4Conn, tlsconfigCopy)
			if err := TlsConn.HandshakeContext(ctx); err != nil {
				return nil, errkit.Wrap(err, "could not tls handshake")
			}
			conn = TlsConn
		} else {
			nativeConn, err := l4.DialContext(ctx, opts.network, hostPort)
			if err != nil {
				return nil, d.handleDialError(err, opts)
			}
			// clone existing tls config
			uTLSConfig := &utls.Config{
				InsecureSkipVerify: tlsconfigCopy.InsecureSkipVerify,
				ServerName:         tlsconfigCopy.ServerName,
				MinVersion:         tlsconfigCopy.MinVersion,
				MaxVersion:         tlsconfigCopy.MaxVersion,
				CipherSuites:       tlsconfigCopy.CipherSuites,
			}
			var uTLSConn *utls.UConn
			switch opts.impersonateStrategy {
			case impersonate.Random:
				uTLSConn = utls.UClient(nativeConn, uTLSConfig, utls.HelloRandomized)
			case impersonate.Custom:
				uTLSConn = utls.UClient(nativeConn, uTLSConfig, utls.HelloCustom)
				clientHelloSpec := utls.ClientHelloSpec(ptrutil.Safe(opts.impersonateIdentity))
				if err := uTLSConn.ApplyPreset(&clientHelloSpec); err != nil {
					return nil, err
				}
			case impersonate.Chrome:
				uTLSConn = utls.UClient(nativeConn, uTLSConfig, utls.HelloChrome_106_Shuffle)
			}
			if err := uTLSConn.Handshake(); err != nil {
				return nil, err
			}
			conn = uTLSConn
		}
	} else if opts.shouldUseZTLS {
		ztlsconfigCopy := opts.ztlsconfig.Clone()
		switch {
		case d.options.SNIName != "":
			ztlsconfigCopy.ServerName = d.options.SNIName
		case ctx.Value(SniName) != nil:
			sniName := ctx.Value(SniName).(string)
			ztlsconfigCopy.ServerName = sniName
		case !iputil.IsIP(opts.hostname):
			ztlsconfigCopy.ServerName = opts.hostname
		}
		l4Conn, err := l4.DialContext(ctx, opts.network, hostPort)
		if err != nil {
			return nil, d.handleDialError(err, opts)
		}
		ztlsConn := ztls.Client(l4Conn, ztlsconfigCopy)
		_, err = ctxutil.ExecFuncWithTwoReturns(ctx, func() (bool, error) {
			// run this in goroutine as select since this does not support context
			return true, ztlsConn.Handshake()
		})
		if err != nil {
			return nil, err
		}
		conn = ztlsConn
	} else {
		if d.proxyDialer != nil {
			dialer := *d.proxyDialer
			// timeout not working for socks5 proxy dialer
			// tying to handle it here
			connectionCh := make(chan net.Conn, 1)
			errCh := make(chan error, 1)
			go func() {
				conn, err = dialer.Dial(opts.network, hostPort)
				if err != nil {
					errCh <- err
					return
				}
				connectionCh <- conn
			}()
			// using timer as time.After is not recovered gy GC
			dialerTime := time.NewTimer(d.options.DialerTimeout)
			defer dialerTime.Stop()
			select {
			case <-dialerTime.C:
				return nil, fmt.Errorf("timeout after %v", d.options.DialerTimeout)
			case conn = <-connectionCh:
			case err = <-errCh:
			}
			err = d.handleDialError(err, opts)
		} else {
			conn, err = l4.DialContext(ctx, opts.network, hostPort)
			err = d.handleDialError(err, opts)
		}
	}
	// fallback to ztls  in case of handshake error with chrome ciphers
	// ztls fallback can either be disabled by setting env variable DISABLE_ZTLS_FALLBACK=true or by setting DisableZtlsFallback=true in options
	if err != nil && !errkit.Is(err, os.ErrDeadlineExceeded) && !(d.options.DisableZtlsFallback && disableZTLSFallback) { //nolint
		var ztlsconfigCopy *ztls.Config
		if opts.shouldUseZTLS {
			ztlsconfigCopy = opts.ztlsconfig.Clone()
		} else {
			if opts.tlsconfig == nil {
				opts.tlsconfig = &tls.Config{
					Renegotiation:      tls.RenegotiateOnceAsClient,
					MinVersion:         tls.VersionTLS10,
					InsecureSkipVerify: true,
				}
			}
			ztlsconfigCopy, err = AsZTLSConfig(opts.tlsconfig)
			if err != nil {
				return nil, errkit.Wrap(err, "could not convert tls config to ztls config")
			}
		}
		ztlsconfigCopy.CipherSuites = ztls.ChromeCiphers
		l4Conn, err := l4.DialContext(ctx, opts.network, hostPort)
		if err != nil {
			return nil, d.handleDialError(err, opts)
		}
		ztlsConn := ztls.Client(l4Conn, ztlsconfigCopy)
		_, err = ctxutil.ExecFuncWithTwoReturns(ctx, func() (bool, error) {
			// run this in goroutine as select since this does not support context
			return true, ztlsConn.Handshake()
		})
		if err != nil {
			return nil, err
		}
		conn = ztlsConn
	}
	return
}

// handleDialError is a helper function to handle dial errors
// it also adds address attribute to the error
func (d *Dialer) handleDialError(err error, opts *dialOptions) error {
	if err == nil {
		return nil
	}
	errx := errkit.FromError(err)
	errx = errx.SetAttr(slog.Any("address", opts.logAddress())) //nolint (ref https://github.com/projectdiscovery/utils/issues/657)

	if errx.Kind() == errkit.ErrKindUnknown {
		if errx.Cause() != nil && strings.Contains(errx.Cause().Error(), "i/o timeout") {
			// mark timeout errors as temporary
			errx = errx.SetKind(errkit.ErrKindNetworkTemporary)

			if d.dialTimeoutErrors != nil {
				count, err := d.dialTimeoutErrors.GetIFPresent(opts.connHash())
				if err != nil {
					count = &atomic.Uint32{}
					count.Store(1)
				} else {
					count.Add(1)
				}
				_ = d.dialTimeoutErrors.Set(opts.connHash(), count)

				// update them to permament if they happened multiple times within 30s
				if count.Load() > uint32(d.options.MaxTemporaryErrors) {
					errx = errx.ResetKind().SetKind(errkit.ErrKindNetworkPermanent)
				}
			}
		}
	}
	return errx
}
