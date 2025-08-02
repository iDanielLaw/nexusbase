package core

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// Regex for metric and label names: ^[\p{L}\p{M}_:][\p{L}\p{M}\p{N}_:\.]*$
// Must start with a Unicode letter, underscore (`_`), or colon (`:`).
// Subsequent characters can be Unicode letters, combining marks, digits, underscores (`_`), colons (`:`), or dots (`.`).
var metricLabelNamePattern = regexp.MustCompile(`^[\p{L}\p{M}_:][\p{L}\p{M}\p{N}_:\.]*$`)

// Reserved label names start with __
const reservedLabelNamePrefix = "__"

// Validator provides cached validation for metrics and labels.
type Validator struct {
	mu    sync.RWMutex
	cache map[string]error // Cache validation results to avoid repeated regex matching.
}

// NewValidator creates a new validator with an initialized cache.
func NewValidator() *Validator {
	return &Validator{
		cache: make(map[string]error),
	}
}

// ValidateMetricName checks if the metric name is valid, using a cache.
func (v *Validator) ValidateMetricName(metric string) error {
	v.mu.RLock()
	err, found := v.cache["metric:"+metric]
	v.mu.RUnlock()
	if found {
		return err // Return cached result
	}

	var validationErr error
	if metric == "" {
		validationErr = &ValidationError{Message: "cannot be empty", Field: "metric", Value: metric}
	} else if !metricLabelNamePattern.MatchString(metric) {
		validationErr = &ValidationError{Message: fmt.Sprintf("does not match pattern '%s'", metricLabelNamePattern.String()), Field: "metric", Value: metric}
	}

	v.mu.Lock()
	v.cache["metric:"+metric] = validationErr
	v.mu.Unlock()

	return validationErr
}

// ValidateLabelName checks if the label name is valid, using a cache.
func (v *Validator) ValidateLabelName(name string) error {
	v.mu.RLock()
	err, found := v.cache["label:"+name]
	v.mu.RUnlock()
	if found {
		return err
	}

	var validationErr error
	if name == "" {
		validationErr = &ValidationError{Message: "cannot be empty", Field: "label_name", Value: name}
	} else if !metricLabelNamePattern.MatchString(name) {
		validationErr = &ValidationError{Message: fmt.Sprintf("does not match pattern '%s'", metricLabelNamePattern.String()), Field: "label_name", Value: name}
	} else if strings.HasPrefix(name, reservedLabelNamePrefix) {
		validationErr = &ValidationError{Message: fmt.Sprintf("is reserved (starts with '%s')", reservedLabelNamePrefix), Field: "label_name", Value: name}
	}

	v.mu.Lock()
	v.cache["label:"+name] = validationErr
	v.mu.Unlock()

	return validationErr
}

func (v *Validator) ValidateInternalLabelName(name string) error {
	v.mu.RLock()
	err, found := v.cache["label:"+name]
	v.mu.RUnlock()
	if found {
		return err
	}

	var validationErr error
	if name == "" {
		validationErr = &ValidationError{Message: "cannot be empty", Field: "label_name", Value: name}
	} else if !metricLabelNamePattern.MatchString(name) {
		validationErr = &ValidationError{Message: fmt.Sprintf("does not match pattern '%s'", metricLabelNamePattern.String()), Field: "label_name", Value: name}
	}

	v.mu.Lock()
	v.cache["label:"+name] = validationErr
	v.mu.Unlock()

	return validationErr
}
