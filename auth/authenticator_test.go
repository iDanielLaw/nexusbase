package auth

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newTestAuthenticator creates an authenticator with a pre-populated user file.
func newTestAuthenticator(t *testing.T, hashType HashType) core.IAuthenticator {
	t.Helper()
	tempDir := t.TempDir()
	userFilePath := filepath.Join(tempDir, "test_users.db")

	writerHash, _ := HashPassword("writer_pass", hashType)
	readerHash, _ := HashPassword("reader_pass", hashType)

	users := map[string]UserRecord{
		"writer": {Username: "writer", PasswordHash: writerHash, Role: RoleWriter},
		"reader": {Username: "reader", PasswordHash: readerHash, Role: RoleReader},
	}

	if err := WriteUserFile(userFilePath, users, hashType); err != nil {
		t.Fatalf("Failed to write user file for test: %v", err)
	}

	authN, err := NewAuthenticator(userFilePath, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("NewAuthenticator failed: %v", err)
	}
	return authN
}

func TestAuthenticator_Authenticate(t *testing.T) {
	testCases := []struct {
		name     string
		hashType HashType
	}{
		{"bcrypt", HashTypeBcrypt},
		{"sha256", HashTypeSHA256},
		{"sha512", HashTypeSHA512},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			authN := newTestAuthenticator(t, tc.hashType)

			// --- Sub-test cases for Authenticate ---
			authTestCases := []struct {
				subName       string
				username      string
				password      string
				expectedCode  codes.Code
				expectedRole  string
				expectUserCtx bool
			}{
				{"valid_writer", "writer", "writer_pass", codes.OK, RoleWriter, true},
				{"valid_reader", "reader", "reader_pass", codes.OK, RoleReader, true},
				{"invalid_password", "writer", "wrong_pass", codes.Unauthenticated, "", false},
				{"invalid_username", "nonexistent", "any_pass", codes.Unauthenticated, "", false},
				{"empty_credentials", "", "", codes.Unauthenticated, "", false},
				{"no_metadata", "writer", "writer_pass", codes.Unauthenticated, "", false},
				{"no_auth_header", "writer", "writer_pass", codes.Unauthenticated, "", false},
				{"malformed_header_scheme", "writer", "writer_pass", codes.Unauthenticated, "", false},
				{"malformed_header_base64", "writer", "writer_pass", codes.Unauthenticated, "", false},
				{"malformed_header_format", "writer", "writer_pass", codes.Unauthenticated, "", false},
			}

			for _, authTc := range authTestCases {
				t.Run(authTc.subName, func(t *testing.T) {
					var ctx context.Context
					switch authTc.subName {
					case "no_metadata":
						ctx = context.Background()
					case "no_auth_header":
						ctx = metadata.NewIncomingContext(context.Background(), metadata.MD{})
					case "malformed_header_scheme":
						ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer some-token"))
					case "malformed_header_base64":
						ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic not-base-64-%%%"))
					case "malformed_header_format":
						ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("justusername"))))
					default:
						authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(authTc.username+":"+authTc.password))
						ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", authHeader))
					}

					newCtx, err := authN.Authenticate(ctx)
					st, _ := status.FromError(err)
					if st.Code() != authTc.expectedCode {
						t.Errorf("Expected status code %v, got %v (err: %v)", authTc.expectedCode, st.Code(), err)
					}

					if authTc.expectUserCtx {
						if newCtx == nil {
							t.Fatal("Expected a new context, but got nil")
						}
						user, ok := newCtx.Value(UserContextKey).(User)
						if !ok {
							t.Fatal("User not found in context or wrong type")
						}
						if user.Username != authTc.username {
							t.Errorf("Username in context mismatch: got %s, want %s", user.Username, authTc.username)
						}
						if user.Role != authTc.expectedRole {
							t.Errorf("Role in context mismatch: got %s, want %s", user.Role, authTc.expectedRole)
						}
					}
				})
			}
		})
	}
}

func TestAuthenticator_Authorize(t *testing.T) {
	authN := newTestAuthenticator(t, HashTypeBcrypt) // Hash type doesn't matter for Authorize tests

	writerUser := User{Username: "writer", Role: RoleWriter}
	readerUser := User{Username: "reader", Role: RoleReader}

	t.Run("WriterCanWrite", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserContextKey, writerUser)
		err := authN.Authorize(ctx, RoleWriter)
		if err != nil {
			t.Errorf("writer should be able to perform 'writer' role operation, but got err: %v", err)
		}
	})

	t.Run("WriterCanRead", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserContextKey, writerUser)
		err := authN.Authorize(ctx, RoleReader)
		if err != nil {
			t.Errorf("writer should be able to perform 'reader' role operation, but got err: %v", err)
		}
	})

	t.Run("ReaderCanRead", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserContextKey, readerUser)
		err := authN.Authorize(ctx, RoleReader)
		if err != nil {
			t.Errorf("reader should be able to perform 'reader' role operation, but got err: %v", err)
		}
	})

	t.Run("ReaderCannotWrite", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), UserContextKey, readerUser)
		err := authN.Authorize(ctx, RoleWriter)
		if err == nil {
			t.Error("reader should not be able to perform 'writer' role operation, but got no error")
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("Expected PermissionDenied error, but got code %v", status.Code(err))
		}
	})

	t.Run("NoUserInContext", func(t *testing.T) {
		ctx := context.Background() // A context without a user
		err := authN.Authorize(ctx, RoleReader)
		if err == nil {
			t.Error("Expected an error when no user is in context, but got nil")
		}
		if status.Code(err) != codes.Internal {
			t.Errorf("Expected Internal error, but got code %v", status.Code(err))
		}
	})
}

func TestAuthenticator_UnsupportedHashType(t *testing.T) {
	// Manually create an authenticator with an invalid hash type.
	// This simulates a state that should not be possible if created via NewAuthenticator,
	// but tests the defensive default case in Authenticate.
	authN := &Authenticator{
		usersByUsername: map[string]User{
			"testuser": {Username: "testuser", PasswordHash: "somehash", Role: RoleReader},
		},
		hashType: HashTypeUnknown, // Invalid hash type
		logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte("testuser:password"))
	md := metadata.Pairs("authorization", authHeader)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := authN.Authenticate(ctx)

	if err == nil {
		t.Fatal("Expected an error for unsupported hash type, but got nil")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("Expected status code %v, got %v", codes.Internal, st.Code())
	}
}
