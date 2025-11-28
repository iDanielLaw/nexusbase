package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestValidateAfterStart_Debug(t *testing.T) {
	opts := GetBaseOptsForTest(t, "debug")
	eng, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	v := core.NewValidator()
	err = core.ValidateMetricAndTags(v, "", map[string]string{"id": "A"})
	if err == nil {
		t.Fatalf("expected validation error after Start, got nil")
	}
}
