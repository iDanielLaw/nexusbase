package testutil

import (
	"net"
)

// InMemoryListener is a simple net.Listener for in-process tests.
// It uses net.Pipe to produce paired connections; the server receives
// one end via Accept() and the test code obtains the client end with Dial().
type InMemoryListener struct {
	conns  chan net.Conn
	closed chan struct{}
	addr   net.Addr
}

type memAddr string

func (m memAddr) Network() string { return "inmem" }
func (m memAddr) String() string  { return string(m) }

// NewInMemoryListener creates a new in-memory listener with a buffered
// connection queue.
func NewInMemoryListener() *InMemoryListener {
	return &InMemoryListener{
		conns:  make(chan net.Conn, 16),
		closed: make(chan struct{}),
		addr:   memAddr("inmemory"),
	}
}

func (l *InMemoryListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *InMemoryListener) Close() error {
	select {
	case <-l.closed:
		return nil
	default:
	}

	close(l.closed)

	// Drain any pending connections and close them to avoid leaks.
	for {
		select {
		case c := <-l.conns:
			if c != nil {
				_ = c.Close()
			}
		default:
			return nil
		}
	}
}

func (l *InMemoryListener) Addr() net.Addr { return l.addr }

// Dial creates a client-side connection paired with a server-side
// connection that will be returned by the next Accept().
func (l *InMemoryListener) Dial() (net.Conn, error) {
	c1, c2 := net.Pipe()
	select {
	case l.conns <- c1:
		return c2, nil
	case <-l.closed:
		c1.Close()
		c2.Close()
		return nil, net.ErrClosed
	}
}
