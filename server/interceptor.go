package server

import (
	"context"
	"log/slog"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/auth"
	"github.com/INLOpen/nexusbase/core"
	"google.golang.org/grpc"
)

// AuthInterceptor provides gRPC interceptors for authentication and authorization.
type AuthInterceptor struct {
	authenticator core.IAuthenticator
	logger        *slog.Logger
}

// NewAuthInterceptor creates a new AuthInterceptor.
func NewAuthInterceptor(authenticator core.IAuthenticator, logger *slog.Logger) *AuthInterceptor {
	return &AuthInterceptor{
		authenticator: authenticator,
		logger:        logger.With("component", "AuthInterceptor"),
	}
}

// Unary returns a gRPC unary server interceptor.
func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Authenticate the request
		newCtx, err := i.authenticator.Authenticate(ctx)
		if err != nil {
			i.logger.Warn("Unary authentication failed", "method", info.FullMethod, "error", err)
			return nil, err
		}

		// Authorize based on the required role for the method
		requiredRole := i.getRequiredRole(info.FullMethod)
		if err := i.authenticator.Authorize(newCtx, requiredRole); err != nil {
			i.logger.Warn("Unary authorization failed", "method", info.FullMethod, "error", err)
			return nil, err
		}

		// Call the original handler with the new context containing user info
		return handler(newCtx, req)
	}
}

// Stream returns a gRPC stream server interceptor.
func (i *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Authenticate the request from the stream's context
		newCtx, err := i.authenticator.Authenticate(ss.Context())
		if err != nil {
			i.logger.Warn("Stream authentication failed", "method", info.FullMethod, "error", err)
			return err
		}

		// Authorize based on the required role for the method
		requiredRole := i.getRequiredRole(info.FullMethod)
		if err := i.authenticator.Authorize(newCtx, requiredRole); err != nil {
			i.logger.Warn("Stream authorization failed", "method", info.FullMethod, "error", err)
			return err
		}

		// Wrap the server stream with the new context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          newCtx,
		}

		// Call the original handler with the wrapped stream
		return handler(srv, wrappedStream)
	}
}

// getRequiredRole determines the required role for a given gRPC method.
func (i *AuthInterceptor) getRequiredRole(fullMethod string) string {
	switch fullMethod {
	// Write operations require RoleWriter
	case tsdb.TSDBService_Put_FullMethodName,
		tsdb.TSDBService_PutBatch_FullMethodName,
		tsdb.TSDBService_Delete_FullMethodName,
		tsdb.TSDBService_DeleteSeries_FullMethodName,
		tsdb.TSDBService_DeletesByTimeRange_FullMethodName,
		tsdb.TSDBService_CreateSnapshot_FullMethodName,
		tsdb.TSDBService_ForceFlush_FullMethodName:
		return auth.RoleWriter

	// Read and Subscribe operations require at least RoleReader
	case tsdb.TSDBService_Get_FullMethodName,
		tsdb.TSDBService_Query_FullMethodName,
		tsdb.TSDBService_GetSeriesByTags_FullMethodName,
		tsdb.TSDBService_Subscribe_FullMethodName:
		return auth.RoleReader

	default:
		// Default to the most restrictive role if method is unknown
		return auth.RoleWriter
	}
}

// wrappedServerStream is a helper struct to wrap a grpc.ServerStream
// and overwrite its Context() method.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapped context.
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
