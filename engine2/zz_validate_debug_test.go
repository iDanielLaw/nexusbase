package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestCoreValidate_Debug(t *testing.T) {
	v := core.NewValidator()
	if err := core.ValidateMetricAndTags(v, "", map[string]string{"id": "A"}); err == nil {
		t.Fatalf("expected error for empty metric, got nil")
	}
}
