package core

import (
	"context"

	"google.golang.org/grpc"
)

type IAuthenticator interface {
	Authenticate(ctx context.Context) (context.Context, error)
	Authorize(ctx context.Context, requiredRole string) error
	UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
	StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error

	AuthenticateUserPass(username, password string) error
}
