package memberlist

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	cortextls "github.com/cortexproject/cortex/pkg/util/tls"
)

type messageType uint8

const (
	_ messageType = iota // don't use 0
	packet
	stream
)

const zeroZeroZeroZero = "0.0.0.0"

// TCPTransportConfig is a configuration structure for creating new TCPTransport.
type TCPTransportConfig struct {
	// BindAddrs is a list of addresses to bind to.
	BindAddrs flagext.StringSlice `yaml:"bind_addr"`

	// BindPort is the port to listen on, for each address above.
	BindPort int `yaml:"bind_port"`

	// Timeout used when making connections to other nodes to send packet.
	// Zero = no timeout
	PacketDialTimeout time.Duration `yaml:"packet_dial_timeout"`

	// Timeout for writing packet data. Zero = no timeout.
	PacketWriteTimeout time.Duration `yaml:"packet_write_timeout"`

	// Timeout for reading inbound packet data. Zero = no timeout.
	PacketReadTimeout time.Duration `yaml:"packet_read_timeout"`

	// Maximum size in bytes of a single inbound packet. Zero = no limit.
	MaxPacketSize int64 `yaml:"max_packet_size"`

	// Maximum number of concurrent inbound TCP connections. Zero = no limit.
	MaxConcurrentConnections int `yaml:"max_concurrent_connections"`

	// Transport logs lot of messages at debug level, so it deserves an extra flag for turning it on
	TransportDebug bool `yaml:"-"`

	// Where to put custom metrics. nil = don't register.
	MetricsRegisterer prometheus.Registerer `yaml:"-"`
	MetricsNamespace  string                `yaml:"-"`

	TLSEnabled bool                   `yaml:"tls_enabled"`
	TLS        cortextls.ClientConfig `yaml:",inline"`
}

func (cfg *TCPTransportConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "")
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *TCPTransportConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	// "Defaults to hostname" -- memberlist sets it to hostname by default.
	f.Var(&cfg.BindAddrs, prefix+"memberlist.bind-addr", "IP address to listen on for gossip messages. Multiple addresses may be specified. Defaults to 0.0.0.0")
	f.IntVar(&cfg.BindPort, prefix+"memberlist.bind-port", 7946, "Port to listen on for gossip messages.")
	f.DurationVar(&cfg.PacketDialTimeout, prefix+"memberlist.packet-dial-timeout", 5*time.Second, "Timeout used when connecting to other nodes to send packet.")
	f.DurationVar(&cfg.PacketWriteTimeout, prefix+"memberlist.packet-write-timeout", 5*time.Second, "Timeout for writing 'packet' data.")
	f.DurationVar(&cfg.PacketReadTimeout, prefix+"memberlist.packet-read-timeout", 5*time.Second, "Timeout for reading packet data from inbound connections. 0 = no limit.")
	f.Int64Var(&cfg.MaxPacketSize, prefix+"memberlist.max-packet-size", 1*1024*1024 /*1MB*/, "Maximum size in bytes of an inbound gossip packet. 0 = no limit.")
	f.IntVar(&cfg.MaxConcurrentConnections, prefix+"memberlist.max-concurrent-connections", 100, "Maximum number of concurrent inbound TCP connections. 0 = no limit.")
	f.BoolVar(&cfg.TransportDebug, prefix+"memberlist.transport-debug", false, "Log debug transport messages. Note: global log.level must be at debug level as well.")

	f.BoolVar(&cfg.TLSEnabled, prefix+"memberlist.tls-enabled", false, "Enable TLS on the memberlist transport layer.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix+"memberlist", f)
}

// TCPTransport is a memberlist.Transport implementation that uses TCP for both packet and stream
// operations ("packet" and "stream" are terms used by memberlist).
// It uses a new TCP connections for each operation. There is no connection reuse.
type TCPTransport struct {
	cfg          TCPTransportConfig
	logger       log.Logger
	packetCh     chan *memberlist.Packet
	connCh       chan net.Conn
	wg           sync.WaitGroup
	tcpListeners []net.Listener
	tlsConfig    *tls.Config

	// connSemaphore limits the number of concurrent inbound TCP connections.
	connSemaphore *semaphore.Weighted

	shutdown atomic.Int32

	advertiseMu   sync.RWMutex
	advertiseAddr string

	// metrics
	incomingStreams      prometheus.Counter
	outgoingStreams      prometheus.Counter
	outgoingStreamErrors prometheus.Counter

	receivedPackets       prometheus.Counter
	receivedPacketsBytes  prometheus.Counter
	receivedPacketsErrors prometheus.Counter
	sentPackets           prometheus.Counter
	sentPacketsBytes      prometheus.Counter
	sentPacketsErrors     prometheus.Counter
	unknownConnections    prometheus.Counter
	rejectedConnections   prometheus.Counter
	activeConnections     prometheus.Gauge
	packetReceiveDuration prometheus.Histogram
	packetReceiveBytes    prometheus.Histogram
}

// NewTCPTransport returns a new tcp-based transport with the given configuration. On
// success all the network listeners will be created and listening.
func NewTCPTransport(config TCPTransportConfig, logger log.Logger) (*TCPTransport, error) {
	if len(config.BindAddrs) == 0 {
		config.BindAddrs = []string{zeroZeroZeroZero}
	}

	// Build out the new transport.
	var ok bool
	t := TCPTransport{
		cfg:      config,
		logger:   log.With(logger, "component", "memberlist TCPTransport"),
		packetCh: make(chan *memberlist.Packet),
		connCh:   make(chan net.Conn),
	}

	if config.MaxConcurrentConnections > 0 {
		t.connSemaphore = semaphore.NewWeighted(int64(config.MaxConcurrentConnections))
	}

	var err error
	if config.TLSEnabled {
		t.tlsConfig, err = config.TLS.GetTLSConfig()
		if err != nil {
			return nil, errors.Wrap(err, "unable to create TLS config")
		}
	}

	t.registerMetrics(config.MetricsRegisterer)

	// Clean up listeners if there's an error.
	defer func() {
		if !ok {
			_ = t.Shutdown()
		}
	}()

	// Build all the TCP and UDP listeners.
	port := config.BindPort
	for _, addr := range config.BindAddrs {
		ip := net.ParseIP(addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}

		var tcpLn net.Listener
		if config.TLSEnabled {
			tcpLn, err = tls.Listen("tcp", tcpAddr.String(), t.tlsConfig)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to start TLS TCP listener on %q port %d", addr, port)
			}
		} else {
			tcpLn, err = net.ListenTCP("tcp", tcpAddr)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to start TCP listener on %q port %d", addr, port)
			}
		}

		t.tcpListeners = append(t.tcpListeners, tcpLn)

		// If the config port given was zero, use the first TCP listener
		// to pick an available port and then apply that to everything
		// else.
		if port == 0 {
			port = tcpLn.Addr().(*net.TCPAddr).Port
		}
	}

	// Fire them up now that we've been able to create them all.
	for i := 0; i < len(config.BindAddrs); i++ {
		t.wg.Add(1)
		go t.tcpListen(t.tcpListeners[i])
	}

	ok = true
	return &t, nil
}

// tcpListen is a long running goroutine that accepts incoming TCP connections
// and spawns new go routine to handle each connection. This transport uses TCP connections
// for both packet sending and streams.
// (copied from Memberlist net_transport.go)
func (t *TCPTransport) tcpListen(tcpLn net.Listener) {
	defer t.wg.Done()

	// baseDelay is the initial delay after an AcceptTCP() error before attempting again
	const baseDelay = 5 * time.Millisecond

	// maxDelay is the maximum delay after an AcceptTCP() error before attempting again.
	// In the case that tcpListen() is error-looping, it will delay the shutdown check.
	// Therefore, changes to maxDelay may have an effect on the latency of shutdown.
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		conn, err := tcpLn.Accept()
		if err != nil {
			if s := t.shutdown.Load(); s == 1 {
				break
			}

			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			level.Error(t.logger).Log("msg", "Error accepting TCP connection", "err", err)
			time.Sleep(loopDelay)
			continue
		}
		// No error, reset loop delay
		loopDelay = 0

		// Enforce concurrent connection via semaphore.
		if t.connSemaphore != nil {
			if !t.connSemaphore.TryAcquire(1) {
				t.rejectedConnections.Inc()
				level.Debug(t.logger).Log("msg", "max concurrent connections reached, closing connection", "remote", conn.RemoteAddr())
				_ = conn.Close()
				continue
			}
		}

		t.activeConnections.Inc()
		go func() {
			// handleConnection returns true when it wrapped the conn in a
			// semaphoreConn and transferred ownership of the slot (and the
			// activeConnections gauge) to that wrapper (stream path).
			// In that case we must not release here.
			semTransferred := t.handleConnection(conn)
			if !semTransferred {
				if t.connSemaphore != nil {
					t.connSemaphore.Release(1)
				}
				t.activeConnections.Dec()
			}
		}()
	}
}

var noopLogger = log.NewNopLogger()

func (t *TCPTransport) debugLog() log.Logger {
	if t.cfg.TransportDebug {
		return level.Debug(t.logger)
	}
	return noopLogger
}

func (t *TCPTransport) handleConnection(conn net.Conn) (semTransferred bool) {
	t.debugLog().Log("msg", "New connection", "addr", conn.RemoteAddr())

	closeConn := true
	defer func() {
		if closeConn {
			_ = conn.Close()
		}
	}()

	// Apply a read deadline for the entire packet receive so that a slow or
	// adversarial peer cannot hold the goroutine open indefinitely.
	if t.cfg.PacketReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(t.cfg.PacketReadTimeout)); err != nil {
			level.Warn(t.logger).Log("msg", "failed to set read deadline", "err", err, "remote", conn.RemoteAddr())
			return
		}
	}

	// let's read first byte, and determine what to do about this connection
	msgType := []byte{0}
	_, err := io.ReadFull(conn, msgType)
	if err != nil {
		level.Warn(t.logger).Log("msg", "failed to read message type", "err", err, "remote", conn.RemoteAddr())
		return
	}

	if messageType(msgType[0]) == stream {
		t.incomingStreams.Inc()

		// Stream connections are handed off to memberlist which manages them
		// independently – clear the deadline so memberlist can use its own
		// timeouts, then pass the connection over.
		if t.cfg.PacketReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Time{})
		}

		// hand over this connection to memberlist.
		// If the semaphore is active, wrap the conn so that the slot is held
		// for the real lifetime of the stream. The memberlist will close it.
		closeConn = false
		if t.connSemaphore != nil {
			t.connCh <- &semaphoreConn{Conn: conn, sem: t.connSemaphore, activeGauge: t.activeConnections}
		} else {
			t.connCh <- &semaphoreConn{Conn: conn, activeGauge: t.activeConnections}
		}
		semTransferred = true
	} else if messageType(msgType[0]) == packet {
		// it's a memberlist "packet", which contains an address and data.
		t.receivedPackets.Inc()

		packetStart := time.Now()
		// before reading packet, read the address
		addrLengthBuf := []byte{0}
		_, err := io.ReadFull(conn, addrLengthBuf)
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "error while reading node address length from packet", "err", err, "remote", conn.RemoteAddr())
			return
		}

		addrBuf := make([]byte, addrLengthBuf[0])
		_, err = io.ReadFull(conn, addrBuf)
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "error while reading node address from packet", "err", err, "remote", conn.RemoteAddr())
			return
		}

		var reader io.Reader = conn
		if t.cfg.MaxPacketSize > 0 {
			// Read one byte beyond the limit so we can detect oversized packets.
			reader = io.LimitReader(conn, t.cfg.MaxPacketSize+1)
		}
		buf, err := io.ReadAll(reader)
		t.packetReceiveDuration.Observe(time.Since(packetStart).Seconds())
		t.packetReceiveBytes.Observe(float64(len(buf)))
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "error while reading packet data", "err", err, "remote", conn.RemoteAddr())
			return
		}

		// Reject oversized packets
		if t.cfg.MaxPacketSize > 0 && int64(len(buf)) > t.cfg.MaxPacketSize {
			t.receivedPacketsErrors.Inc()
			level.Debug(t.logger).Log("msg", "packet too large, dropping", "size", len(buf), "max", t.cfg.MaxPacketSize, "remote", conn.RemoteAddr())
			return
		}

		if len(buf) < md5.Size {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "not enough data received", "data_length", len(buf), "remote", conn.RemoteAddr())
			return
		}

		receivedDigest := buf[len(buf)-md5.Size:]
		buf = buf[:len(buf)-md5.Size]

		expectedDigest := md5.Sum(buf)

		if !bytes.Equal(receivedDigest, expectedDigest[:]) {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "packet digest mismatch", "expected", fmt.Sprintf("%x", expectedDigest), "received", fmt.Sprintf("%x", receivedDigest), "data_length", len(buf), "remote", conn.RemoteAddr())
			return
		}

		t.debugLog().Log("msg", "Received packet", "addr", addr(addrBuf), "size", len(buf), "hash", fmt.Sprintf("%x", receivedDigest))

		t.receivedPacketsBytes.Add(float64(len(buf)))

		t.packetCh <- &memberlist.Packet{
			Buf:       buf,
			From:      addr(addrBuf),
			Timestamp: time.Now(),
		}
	} else {
		t.unknownConnections.Inc()
		level.Error(t.logger).Log("msg", "unknown message type", "msgType", msgType, "remote", conn.RemoteAddr())
	}
	return
}

type addr string

func (a addr) Network() string {
	return "tcp"
}

func (a addr) String() string {
	return string(a)
}

// semaphoreConn wraps a net.Conn and releases a semaphore slot (if set) and
// decrements the active-connections gauge exactly once when the connection is
// closed. It is used on the stream path to keep both the semaphore slot and
// the gauge accurate for the real lifetime of the connection.
type semaphoreConn struct {
	net.Conn
	sem         *semaphore.Weighted
	activeGauge prometheus.Gauge
	once        sync.Once
}

func (c *semaphoreConn) Close() error {
	c.once.Do(func() {
		if c.sem != nil {
			c.sem.Release(1)
		}
		c.activeGauge.Dec()
	})
	return c.Conn.Close()
}

func (t *TCPTransport) getConnection(addr string, timeout time.Duration) (net.Conn, error) {
	if t.cfg.TLSEnabled {
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, t.tlsConfig)
	}
	return net.DialTimeout("tcp", addr, timeout)
}

// GetAutoBindPort returns the bind port that was automatically given by the
// kernel, if a bind port of 0 was given.
func (t *TCPTransport) GetAutoBindPort() int {
	// We made sure there's at least one TCP listener, and that one's
	// port was applied to all the others for the dynamic bind case.
	return t.tcpListeners[0].Addr().(*net.TCPAddr).Port
}

// FinalAdvertiseAddr is given the user's configured values (which
// might be empty) and returns the desired IP and port to advertise to
// the rest of the cluster.
// (Copied from memberlist' net_transport.go)
func (t *TCPTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {
		// If they've supplied an address, use that.
		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("failed to parse advertise address %q", ip)
		}

		// Ensure IPv4 conversion if necessary.
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		if t.cfg.BindAddrs[0] == zeroZeroZeroZero {
			// Otherwise, if we're not bound to a specific IP, let's
			// use a suitable private IP address.
			var err error
			ip, err = sockaddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("failed to get interface addresses: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("no private IP address found, and explicit IP not provided")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("failed to parse advertise address: %q", ip)
			}
		} else {
			// Use the IP that we're bound to, based on the first
			// TCP listener, which we already ensure is there.
			advertiseAddr = t.tcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		// Use the port we are bound to.
		advertisePort = t.GetAutoBindPort()
	}

	level.Debug(t.logger).Log("msg", "FinalAdvertiseAddr", "advertiseAddr", advertiseAddr.String(), "advertisePort", advertisePort)

	t.setAdvertisedAddr(advertiseAddr, advertisePort)
	return advertiseAddr, advertisePort, nil
}

func (t *TCPTransport) setAdvertisedAddr(advertiseAddr net.IP, advertisePort int) {
	t.advertiseMu.Lock()
	defer t.advertiseMu.Unlock()
	addr := net.TCPAddr{IP: advertiseAddr, Port: advertisePort}
	t.advertiseAddr = addr.String()
}

func (t *TCPTransport) getAdvertisedAddr() string {
	t.advertiseMu.RLock()
	defer t.advertiseMu.RUnlock()
	return t.advertiseAddr
}

// WriteTo is a packet-oriented interface that fires off the given
// payload to the given address.
func (t *TCPTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	t.sentPackets.Inc()
	t.sentPacketsBytes.Add(float64(len(b)))

	err := t.writeTo(b, addr)
	if err != nil {
		t.sentPacketsErrors.Inc()

		logLevel := level.Warn(t.logger)
		if strings.Contains(err.Error(), "connection refused") {
			// The connection refused is a common error that could happen during normal operations when a node
			// shutdown (or crash). It shouldn't be considered a warning condition on the sender side.
			logLevel = t.debugLog()
		}
		logLevel.Log("msg", "WriteTo failed", "addr", addr, "err", err)

		// WriteTo is used to send "UDP" packets. Since we use TCP, we can detect more errors,
		// but memberlist library doesn't seem to cope with that very well. That is why we return nil instead.
		return time.Now(), nil
	}

	return time.Now(), nil
}

func (t *TCPTransport) writeTo(b []byte, addr string) error {
	// Open connection, write packet header and data, data hash, close. Simple.
	c, err := t.getConnection(addr, t.cfg.PacketDialTimeout)
	if err != nil {
		return err
	}

	closed := false
	defer func() {
		if !closed {
			// If we still need to close, then there was another error. Ignore this one.
			_ = c.Close()
		}
	}()

	// Compute the digest *before* setting the deadline on the connection (so that the time
	// it takes to compute the digest is not taken in account).
	// We use md5 as quick and relatively short hash, not in cryptographic context.
	// It's also used to detect if the whole packet has been received on the receiver side.
	digest := md5.Sum(b)

	// Prepare the header *before* setting the deadline on the connection.
	headerBuf := bytes.Buffer{}
	headerBuf.WriteByte(byte(packet))

	// We need to send our address to the other side, otherwise other side can only see IP and port from TCP header.
	// But that doesn't match our node address (new TCP connection has new random port), which confuses memberlist.
	// So we send our advertised address, so that memberlist on the receiving side can match it with correct node.
	// This seems to be important for node probes (pings) done by memberlist.
	ourAddr := t.getAdvertisedAddr()
	if len(ourAddr) > 255 {
		return fmt.Errorf("local address too long")
	}

	headerBuf.WriteByte(byte(len(ourAddr)))
	headerBuf.WriteString(ourAddr)

	if t.cfg.PacketWriteTimeout > 0 {
		deadline := time.Now().Add(t.cfg.PacketWriteTimeout)
		err := c.SetDeadline(deadline)
		if err != nil {
			return fmt.Errorf("setting deadline: %v", err)
		}
	}

	_, err = c.Write(headerBuf.Bytes())
	if err != nil {
		return fmt.Errorf("sending local address: %v", err)
	}

	n, err := c.Write(b)
	if err != nil {
		return fmt.Errorf("sending data: %v", err)
	}
	if n != len(b) {
		return fmt.Errorf("sending data: short write")
	}

	// Append digest.
	n, err = c.Write(digest[:])
	if err != nil {
		return fmt.Errorf("digest: %v", err)
	}
	if n != len(digest) {
		return fmt.Errorf("digest: short write")
	}

	closed = true
	err = c.Close()
	if err != nil {
		return fmt.Errorf("close: %v", err)
	}

	t.debugLog().Log("msg", "WriteTo: packet sent", "addr", addr, "size", len(b), "hash", fmt.Sprintf("%x", digest))
	return nil
}

// PacketCh returns a channel that can be read to receive incoming
// packets from other peers.
func (t *TCPTransport) PacketCh() <-chan *memberlist.Packet {
	return t.packetCh
}

// DialTimeout is used to create a connection that allows memberlist to perform
// two-way communication with a peer.
func (t *TCPTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	t.outgoingStreams.Inc()
	c, err := t.getConnection(addr, timeout)

	if err != nil {
		t.outgoingStreamErrors.Inc()
		return nil, err
	}

	_, err = c.Write([]byte{byte(stream)})
	if err != nil {
		t.outgoingStreamErrors.Inc()
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

// StreamCh returns a channel that can be read to handle incoming stream
// connections from other peers.
func (t *TCPTransport) StreamCh() <-chan net.Conn {
	return t.connCh
}

// Shutdown is called when memberlist is shutting down; this gives the
// transport a chance to clean up any listeners.
func (t *TCPTransport) Shutdown() error {
	// This will avoid log spam about errors when we shut down.
	t.shutdown.Store(1)

	// Rip through all the connections and shut them down.
	for _, conn := range t.tcpListeners {
		_ = conn.Close()
	}

	// Block until all the listener threads have died.
	t.wg.Wait()
	return nil
}

func (t *TCPTransport) registerMetrics(registerer prometheus.Registerer) {
	const subsystem = "memberlist_tcp_transport"

	t.incomingStreams = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "incoming_streams_total",
		Help:      "Number of incoming memberlist streams",
	})

	t.outgoingStreams = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "outgoing_streams_total",
		Help:      "Number of outgoing streams",
	})

	t.outgoingStreamErrors = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "outgoing_stream_errors_total",
		Help:      "Number of errors when opening memberlist stream to another node",
	})

	t.receivedPackets = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_received_total",
		Help:      "Number of received memberlist packets",
	})

	t.receivedPacketsBytes = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_received_bytes_total",
		Help:      "Total bytes received as packets",
	})

	t.receivedPacketsErrors = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_received_errors_total",
		Help:      "Number of errors when receiving memberlist packets",
	})

	t.sentPackets = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_sent_total",
		Help:      "Number of memberlist packets sent",
	})

	t.sentPacketsBytes = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_sent_bytes_total",
		Help:      "Total bytes sent as packets",
	})

	t.sentPacketsErrors = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "packets_sent_errors_total",
		Help:      "Number of errors when sending memberlist packets",
	})

	t.unknownConnections = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "unknown_connections_total",
		Help:      "Number of unknown TCP connections (not a packet or stream)",
	})

	t.rejectedConnections = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "rejected_connections_total",
		Help:      "Number of inbound TCP connections rejected because the concurrent connection limit was reached",
	})

	t.activeConnections = promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "active_connections",
		Help:      "Current number of active inbound TCP connections.",
	})

	t.packetReceiveDuration = promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Namespace:                       t.cfg.MetricsNamespace,
		Subsystem:                       subsystem,
		Name:                            "packet_receive_duration_seconds",
		Help:                            "Duration (in seconds) of inbound packet-type message reads.",
		Buckets:                         prometheus.DefBuckets,
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	})

	t.packetReceiveBytes = promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Namespace:                       t.cfg.MetricsNamespace,
		Subsystem:                       subsystem,
		Name:                            "packet_receive_bytes",
		Help:                            "Distribution of inbound packet sizes in bytes.",
		Buckets:                         prometheus.ExponentialBuckets(64, 4, 8), // 64, 256, 1K, 4K, 16K, 64K, 256K, 1M
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	})
}
