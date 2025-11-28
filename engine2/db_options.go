package engine2

// CreateDBOptions specifies options for CreateDatabase.
type CreateDBOptions struct {
	IfNotExists bool
	Options     map[string]string
}
