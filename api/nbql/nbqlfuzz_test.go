//go:build fuzz
// +build fuzz

package nbql

//go:generate go test -fuzz=Fuzz

import (
	"bytes"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/require"
)

func FuzzDecodePushRequest(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		DecodePushRequest(bytes.NewReader(data))
	})
}

func FuzzDecodeQueryResponse(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		DecodeQueryResponse(bytes.NewReader(data))
	})
}

func FuzzDecodePushsRequest(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		DecodePushsRequest(bytes.NewReader(data))
	})
}

func FuzzDecodeQueryEndResponse(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		DecodeQueryEndResponse(bytes.NewReader(data))
	})
}

func FuzzDecodeErrorMessage(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		DecodeErrorMessage(bytes.NewReader(data))
	})
}

func TestEncodeDecodePushsRequest(t *testing.T) {
	mustNewFieldValues := func(data map[string]interface{}) core.FieldValues {
		fv, err := core.NewFieldValuesFromMap(data)
		require.NoError(t, err)
		return fv
	}
	orig := PushsRequest{
		Items: []PushItem{
			{
				Metric:    "mem",
				Tags:      map[string]string{"service": "api"},
				Timestamp: 42,
				Fields:    mustNewFieldValues(map[string]interface{}{"value": 1.0}),
			},
			{
				Metric:    "cpu",
				Tags:      map[string]string{"service": "worker"},
				Timestamp: 84,
				Fields:    mustNewFieldValues(map[string]interface{}{"load": 0.5}),
			},
		},
	}

	var buf bytes.Buffer
	if err := EncodePushsRequest(&buf, orig); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodePushsRequest(&buf)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Items) != len(orig.Items) {
		t.Errorf("expected %d items, got %d", len(orig.Items), len(decoded.Items))
	}
}

func TestEncodeDecodeQueryEndResponse(t *testing.T) {
	orig := QueryEndResponse{
		Status:    ResponseOK,
		TotalRows: 123,
		Message:   "done",
	}
	var buf bytes.Buffer
	if err := EncodeQueryEndResponse(&buf, orig); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeQueryEndResponse(&buf)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.TotalRows != orig.TotalRows || decoded.Message != orig.Message {
		t.Errorf("decoded data mismatch")
	}
}

func TestEncodeDecodeErrorMessage(t *testing.T) {
	orig := &ErrorMessage{
		Code:    500,
		Message: "internal error",
	}
	var buf bytes.Buffer
	if err := EncodeErrorMessage(&buf, orig); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	err := DecodeErrorMessage(&buf)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	msg, ok := err.(*ErrorMessage)
	if !ok {
		t.Fatalf("unexpected error type: %T", err)
	}
	if msg.Code != orig.Code || msg.Message != orig.Message {
		t.Errorf("decoded error mismatch")
	}
}

func FuzzReadFrame(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		ReadFrame(bytes.NewReader(data))
	})
}
