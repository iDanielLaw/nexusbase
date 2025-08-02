package core

import (
	"errors"
	"fmt"
)

// ValidationError is a custom error type for validation failures.
type ValidationError struct {
	Message string
	Field   string // e.g., "metric", "tag_name", "tag_value"
	Value   string // The invalid value
}

type UnsupportedTypeError struct {
	Message string
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("unsupported type value: %s", e.Message)
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for %s '%s': %s", e.Field, e.Value, e.Message)
}

// IsValidationError checks if an error is a ValidationError.
func IsValidationError(err error) bool {
	var validationError *ValidationError
	// Use errors.As to check if the error (or any error in its chain) is a ValidationError.
	return errors.As(err, &validationError)
}

func IsUnsupportedError(err error) bool {
	var unsupportedError *UnsupportedTypeError
	return errors.As(err, &unsupportedError)
}
