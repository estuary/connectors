package gosnowflake

// ESTUARY DEBUG: this file and every "ESTUARY DEBUG" block in this copy of
// gosnowflake v2.2.0 exist to diagnose failed stage uploads. They log through
// logrus so the lines land in task logs even though the driver's own logger is
// turned off.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// estuaryCountingReader counts the bytes the HTTP client has read from an
// upload body, which bounds from above how many reached the network.
type estuaryCountingReader struct {
	r    io.Reader
	n    atomic.Int64
	eof  atomic.Bool
	last atomic.Int64 // unix nanos of the last read
}

func (c *estuaryCountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	c.last.Store(time.Now().UnixNano())
	if err == io.EOF {
		c.eof.Store(true)
	}
	return n, err
}

// estuaryTrace records the connection-level events of one HTTP request.
type estuaryTrace struct {
	start time.Time
	mu    sync.Mutex
	f     log.Fields
}

func newEstuaryTrace() *estuaryTrace {
	return &estuaryTrace{start: time.Now(), f: log.Fields{}}
}

func (t *estuaryTrace) set(k string, v any) {
	t.mu.Lock()
	t.f[k] = v
	t.mu.Unlock()
}

func (t *estuaryTrace) at(k string) {
	t.set(k, time.Since(t.start).String())
}

func (t *estuaryTrace) fields() log.Fields {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(log.Fields, len(t.f))
	for k, v := range t.f {
		out[k] = v
	}
	return out
}

func (t *estuaryTrace) withContext(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(hostPort string) { t.set("trace.hostPort", hostPort) },
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t.at("trace.dnsDoneAt")
			addrs := make([]string, 0, len(info.Addrs))
			for _, a := range info.Addrs {
				addrs = append(addrs, a.String())
			}
			t.set("trace.dnsAddrs", addrs)
			if info.Err != nil {
				t.set("trace.dnsErr", info.Err.Error())
			}
		},
		ConnectStart: func(network, addr string) {
			t.at("trace.connectStartAt")
			t.set("trace.connectNetwork", network)
		},
		ConnectDone: func(network, addr string, err error) {
			t.at("trace.connectDoneAt")
			t.set("trace.connectAddr", addr)
			if err != nil {
				t.set("trace.connectErr", err.Error())
			}
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			t.at("trace.tlsDoneAt")
			t.set("trace.tlsVersion", tls.VersionName(state.Version))
			t.set("trace.alpn", state.NegotiatedProtocol)
			if err != nil {
				t.set("trace.tlsErr", err.Error())
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.at("trace.gotConnAt")
			t.set("trace.connReused", info.Reused)
			t.set("trace.connWasIdle", info.WasIdle)
			if info.WasIdle {
				t.set("trace.connIdleTime", info.IdleTime.String())
			}
			if info.Conn != nil {
				t.set("trace.localAddr", info.Conn.LocalAddr().String())
				t.set("trace.remoteAddr", info.Conn.RemoteAddr().String())
			}
		},
		WroteHeaders:    func() { t.at("trace.wroteHeadersAt") },
		Wait100Continue: func() { t.at("trace.wait100ContinueAt") },
		Got100Continue:  func() { t.at("trace.got100ContinueAt") },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.at("trace.wroteRequestAt")
			if info.Err != nil {
				t.set("trace.wroteRequestErr", info.Err.Error())
			}
		},
		GotFirstResponseByte: func() { t.at("trace.firstResponseByteAt") },
	})
}

// estuaryRedactURL drops the query string, which carries signatures for
// presigned URLs.
func estuaryRedactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Scheme + "://" + u.Host + u.Path
}

// estuaryErrFields describes every error in err's chain, along with the
// network-level classifications that distinguish one transport failure from
// another.
func estuaryErrFields(err error) log.Fields {
	f := log.Fields{}
	if err == nil {
		return f
	}
	f["err.message"] = err.Error()

	var chain []string
	var walk func(e error, depth int)
	walk = func(e error, depth int) {
		if e == nil || depth > 20 {
			return
		}
		chain = append(chain, fmt.Sprintf("%T: %v", e, e))
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range u.Unwrap() {
				walk(inner, depth+1)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap(), depth+1)
		}
	}
	walk(err, 0)
	f["err.chain"] = chain

	var netErr net.Error
	if errors.As(err, &netErr) {
		f["err.timeout"] = netErr.Timeout()
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		f["err.op"] = opErr.Op
		f["err.net"] = opErr.Net
		if opErr.Addr != nil {
			f["err.addr"] = opErr.Addr.String()
		}
		if opErr.Source != nil {
			f["err.source"] = opErr.Source.String()
		}
	}
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		f["err.syscall"] = sysErr.Syscall
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		f["err.errno"] = fmt.Sprintf("%d (%s)", int(errno), errno.Error())
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		f["err.urlOp"] = urlErr.Op
	}
	f["err.isEOF"] = errors.Is(err, io.EOF)
	f["err.isUnexpectedEOF"] = errors.Is(err, io.ErrUnexpectedEOF)
	f["err.isConnReset"] = errors.Is(err, syscall.ECONNRESET)
	f["err.isBrokenPipe"] = errors.Is(err, syscall.EPIPE)
	f["err.isCanceled"] = errors.Is(err, context.Canceled)
	f["err.isDeadline"] = errors.Is(err, context.DeadlineExceeded)
	f["err.isHandlerTimeout"] = errors.Is(err, http.ErrHandlerTimeout)
	return f
}

// estuaryStageFields describes the stage an upload targets, without
// credentials.
func estuaryStageFields(meta *fileMetadata) log.Fields {
	f := log.Fields{
		"file":            meta.name,
		"srcFileName":     meta.srcFileName,
		"realSrcFileName": meta.realSrcFileName,
		"dstFileName":     meta.dstFileName,
		"srcFileSize":     meta.srcFileSize,
		"uploadSize":      meta.uploadSize,
		"parallel":        meta.parallel,
		"overwrite":       meta.overwrite,
		"hasPresignedURL": meta.presignedURL != nil,
	}
	if si := meta.stageInfo; si != nil {
		f["stage.locationType"] = si.LocationType
		f["stage.location"] = si.Location
		f["stage.region"] = si.Region
		f["stage.endPoint"] = si.EndPoint
		f["stage.storageAccount"] = si.StorageAccount
		f["stage.clientSideEncrypted"] = si.IsClientSideEncrypted
		f["stage.useRegionalURL"] = si.UseRegionalURL
		f["stage.useS3RegionalURL"] = si.UseS3RegionalURL
		f["stage.useVirtualURL"] = si.UseVirtualURL
	}
	if fi, err := os.Stat(meta.realSrcFileName); err == nil {
		f["realSrcFileStatSize"] = fi.Size()
	} else if meta.realSrcFileName != "" {
		f["realSrcFileStatErr"] = err.Error()
	}
	return f
}

// estuaryRateMiBps is the throughput of n bytes over d.
func estuaryRateMiBps(n int64, d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", float64(n)/(1<<20)/d.Seconds())
}
