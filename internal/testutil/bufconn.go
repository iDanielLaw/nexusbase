package testutil

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// NewBufconnListener returns a new bufconn.Listener with a sensible default buffer size.
func NewBufconnListener(bufferSize int) *bufconn.Listener {
	if bufferSize <= 0 {
		bufferSize = 1024 * 1024
	}
	return bufconn.Listen(bufferSize)
}

// BufconnDialOptions returns a slice of grpc.DialOption configured to use the provided
// bufconn listener. Callers can append additional DialOptions as needed.
func BufconnDialOptions(lis *bufconn.Listener) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
	}
}
