package auth

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// User represents an authenticated user with their associated role.
type User struct {
	Username     string
	PasswordHash string
	Role         string
}

// contextKey is a private type to avoid context key collisions.
type contextKey string

const (
	// UserContextKey is the key used to store the User object in the context.
	UserContextKey = contextKey("user")
	// RoleReader allows read-only operations.
	RoleReader = "reader"
	// RoleWriter allows both read and write operations.
	RoleWriter = "writer"
)

// Authenticator handles username/password authentication and role-based authorization.
type Authenticator struct {
	usersByUsername map[string]User
	hashType        HashType // The hash type used for all users in this file
	logger          *slog.Logger
}

// NewAuthenticator creates a new Authenticator from the binary user file.
func NewAuthenticator(userFilePath string, logger *slog.Logger) (core.IAuthenticator, error) {
	userRecords, hashType, err := ReadUserFile(userFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not load user database: %w", err)
	}

	userMap := make(map[string]User, len(userRecords))
	for _, u := range userRecords {
		userMap[u.Username] = User{Username: u.Username, PasswordHash: u.PasswordHash, Role: u.Role}
	}

	return &Authenticator{
		usersByUsername: userMap,
		hashType:        hashType,
		logger:          logger.With("component", "Authenticator"),
	}, nil
}

func (a *Authenticator) checkAuthentication(username, password string) (User, error) {
	user, ok := a.usersByUsername[username]
	if !ok {
		a.logger.Warn("Authentication failed: invalid username.", "username", username)
		return User{}, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	// Compare password based on the file's hash type
	var match bool
	switch a.hashType {
	case HashTypeBcrypt:
		err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password))
		match = (err == nil)
	case HashTypeSHA256:
		h := sha256.New()
		h.Write([]byte(password))
		hashedPasswordBytes := h.Sum(nil)
		storedHashBytes, err := hex.DecodeString(user.PasswordHash)
		match = err == nil && subtle.ConstantTimeCompare(hashedPasswordBytes, storedHashBytes) == 1
	case HashTypeSHA512:
		h := sha512.New()
		h.Write([]byte(password))
		hashedPasswordBytes := h.Sum(nil)
		storedHashBytes, err := hex.DecodeString(user.PasswordHash)
		match = err == nil && subtle.ConstantTimeCompare(hashedPasswordBytes, storedHashBytes) == 1
	default:
		// This should not happen if NewAuthenticator validates the hash type
		return User{}, status.Error(codes.Internal, "server configured with unsupported password hash type")
	}

	if !match {
		a.logger.Warn("Authentication failed: password mismatch.", "username", username)
		return User{}, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	return user, nil
}

// Authenticate extracts Basic Auth credentials from the gRPC context, validates them,
// and returns a new context with the authenticated user's information.
func (a *Authenticator) Authenticate(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing credentials")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing credentials")
	}
	authHeader := values[0]

	if !strings.HasPrefix(authHeader, "Basic ") {
		return nil, status.Error(codes.Unauthenticated, "invalid authorization header format")
	}
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid base64 in authorization header")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, status.Error(codes.Unauthenticated, "invalid basic auth format")
	}

	user, err := a.checkAuthentication(parts[0], parts[1])
	if err != nil {
		return nil, err
	}

	newCtx := context.WithValue(ctx, UserContextKey, user)
	return newCtx, nil
}

// Authorize checks if the user in the context has the required role.
func (a *Authenticator) Authorize(ctx context.Context, requiredRole string) error {
	user, ok := ctx.Value(UserContextKey).(User)
	if !ok {
		// This should not happen if Authenticate is called first.
		return status.Error(codes.Internal, "no user information in context")
	}

	if user.Role == RoleWriter || (user.Role == RoleReader && requiredRole == RoleReader) {
		return nil // Authorized
	}

	return status.Error(codes.PermissionDenied, fmt.Sprintf("user '%s' with role '%s' is not authorized for this operation (requires role '%s')", user.Username, user.Role, requiredRole))
}

// UnaryInterceptor is a gRPC unary server interceptor for authentication.
func (a *Authenticator) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if a == nil { // If auth is disabled, just proceed.
		return handler(ctx, req)
	}
	newCtx, err := a.Authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(newCtx, req)
}

// StreamInterceptor is a gRPC stream server interceptor for authentication.
func (a *Authenticator) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if a == nil { // If auth is disabled, just proceed.
		return handler(srv, ss)
	}
	newCtx, err := a.Authenticate(ss.Context())
	if err != nil {
		return err
	}
	wrappedStream := &wrappedServerStream{ServerStream: ss, newCtx: newCtx}
	return handler(srv, wrappedStream)
}

func (a *Authenticator) AuthenticateUserPass(username, password string) error {
	_, err := a.checkAuthentication(username, password)
	if err != nil {
		return err
	}

	return nil

}

// wrappedServerStream wraps an existing grpc.ServerStream with a new context.
type wrappedServerStream struct {
	grpc.ServerStream
	newCtx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.newCtx
}
