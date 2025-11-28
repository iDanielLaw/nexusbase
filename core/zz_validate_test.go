package core

import (
	"testing"
)

func TestValidateMetricAndTags_Debug(t *testing.T) {
	v := NewValidator()
	if err := ValidateMetricAndTags(v, "", map[string]string{"id": "A"}); err == nil {
		t.Fatalf("expected error for empty metric, got nil")
	}
}
