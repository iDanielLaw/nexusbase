package auth

import (
	"context"

	"github.com/INLOpen/nexusbase/core"
	"google.golang.org/grpc"
)

type NonAuthenticator struct{}

var _ core.IAuthenticator = (*NonAuthenticator)(nil)

func NewNonAuthenticator() core.IAuthenticator {
	return &NonAuthenticator{}
}

func (a *NonAuthenticator) Authenticate(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (a *NonAuthenticator) Authorize(ctx context.Context, requiredRole string) error {
	return nil
}

func (a *NonAuthenticator) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return handler(ctx, req)
}

func (a *NonAuthenticator) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}

func (a *NonAuthenticator) AuthenticateUserPass(username, password string) error {
	return nil
}
