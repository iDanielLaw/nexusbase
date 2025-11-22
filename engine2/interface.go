package engine2

import "context"

// CreateDBOptions holds options for creating a database.
type CreateDBOptions struct {
	IfNotExists bool
	Options     map[string]string
}

// Engine2Interface defines a minimal public API for the new engine2.
type Engine2Interface interface {
	CreateDatabase(ctx context.Context, name string, opts CreateDBOptions) error
	GetDataRoot() string
}
