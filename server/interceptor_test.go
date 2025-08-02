package server

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// mockUnaryHandler is a dummy handler for testing the interceptor.
func mockUnaryHandler(ctx context.Context, req interface{}) (interface{}, error) {
	return "handler called", nil
}

// setupInterceptor creates an AuthInterceptor for testing.
func setupInterceptor(t *testing.T) *AuthInterceptor {
	t.Helper()
	tempDir := t.TempDir()
	userFilePath := filepath.Join(tempDir, "test_users.db")

	writerHash, _ := auth.HashPassword("writer_pass", auth.HashTypeBcrypt)
	readerHash, _ := auth.HashPassword("reader_pass", auth.HashTypeBcrypt)

	users := map[string]auth.UserRecord{
		"writer": {Username: "writer", PasswordHash: writerHash, Role: auth.RoleWriter},
		"reader": {Username: "reader", PasswordHash: readerHash, Role: auth.RoleReader},
	}

	if err := auth.WriteUserFile(userFilePath, users, auth.HashTypeBcrypt); err != nil {
		t.Fatalf("Failed to write user file for test: %v", err)
	}

	authN, err := auth.NewAuthenticator(userFilePath, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("NewAuthenticator failed: %v", err)
	}

	return NewAuthInterceptor(authN, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestAuthInterceptor_Unary(t *testing.T) {
	interceptor := setupInterceptor(t)
	unaryInterceptor := interceptor.Unary()

	testCases := []struct {
		name         string
		method       string // Full gRPC method name
		username     string
		password     string
		expectedCode codes.Code
	}{
		// Write operations
		{"writer_can_put", tsdb.TSDBService_Put_FullMethodName, "writer", "writer_pass", codes.OK},
		{"reader_cannot_put", tsdb.TSDBService_Put_FullMethodName, "reader", "reader_pass", codes.PermissionDenied},
		{"invalid_user_cannot_put", tsdb.TSDBService_Put_FullMethodName, "writer", "wrong_pass", codes.Unauthenticated},

		// Read operations
		{"writer_can_get", tsdb.TSDBService_Get_FullMethodName, "writer", "writer_pass", codes.OK},
		{"reader_can_get", tsdb.TSDBService_Get_FullMethodName, "reader", "reader_pass", codes.OK},
		{"invalid_user_cannot_get", tsdb.TSDBService_Get_FullMethodName, "writer", "wrong_pass", codes.Unauthenticated},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var ctx context.Context
			if tc.username != "" || tc.password != "" {
				authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(tc.username+":"+tc.password))
				md := metadata.Pairs("authorization", authHeader)
				ctx = metadata.NewIncomingContext(context.Background(), md)
			} else {
				ctx = context.Background() // No credentials
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: tc.method,
			}

			_, err := unaryInterceptor(ctx, nil, info, mockUnaryHandler)

			st, _ := status.FromError(err)
			if st.Code() != tc.expectedCode {
				t.Errorf("Expected status code %v, got %v (err: %v)", tc.expectedCode, st.Code(), err)
			}
		})
	}
}
