package nbql

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewFieldValues(t *testing.T, data map[string]interface{}) core.FieldValues {
	fv, err := core.NewFieldValuesFromMap(data)
	require.NoError(t, err)
	return fv
}

func TestPushRequest_EncodeDecode(t *testing.T) {
	// Helper to create FieldValues for tests
	mustNewFieldValues := func(data map[string]interface{}) core.FieldValues {
		fv, err := core.NewFieldValuesFromMap(data)
		require.NoError(t, err)
		return fv
	}

	testCases := []struct {
		name    string
		request PushRequest
	}{
		{
			name: "request with single float field (legacy style)",
			request: PushRequest{
				Metric:    "cpu.usage",
				Tags:      map[string]string{"host": "server-1", "region": "us-east"},
				Timestamp: 1672531200000000000,
				Fields:    mustNewFieldValues(map[string]interface{}{"value": 85.5}),
			},
		},
		{
			name: "request with multiple fields",
			request: PushRequest{
				Metric:    "http.request",
				Tags:      map[string]string{"path": "/api/v1", "status_code": "200"},
				Timestamp: 1672531201000000000,
				Fields: mustNewFieldValues(map[string]interface{}{
					"latency_ms": 123.45,
					"success":    true,
					"user":       "test",
				}),
			},
		},
		{
			name: "request with no tags and no fields",
			request: PushRequest{
				Metric:    "system.heartbeat",
				Tags:      nil,
				Timestamp: 1672531202000000000,
				Fields:    nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Encode
			err := EncodePushRequest(&buf, tc.request)
			if err != nil {
				t.Fatalf("EncodePushRequest failed: %v", err)
			}

			// Decode
			decodedRequest, err := DecodePushRequest(&buf)
			if err != nil {
				t.Fatalf("DecodePushRequest failed: %v", err)
			}

			// Compare
			if !reflect.DeepEqual(tc.request, decodedRequest) {
				t.Errorf("Decoded request does not match original.\nOriginal: %+v\nDecoded:  %+v", tc.request, decodedRequest)
			}
		})
	}
}

func TestFrame_ReadWrite(t *testing.T) {
	t.Run("Successful Roundtrip", func(t *testing.T) {
		var buf bytes.Buffer
		payload := []byte("hello integrity")
		cmdType := CommandPush

		// Write the frame with CRC
		err := WriteFrame(&buf, cmdType, payload)
		assert.NoError(t, err)

		// Now, read it back using the new ReadFrame function
		readCmdType, readPayload, err := ReadFrame(&buf)
		assert.NoError(t, err)

		assert.Equal(t, cmdType, readCmdType, "CommandType mismatch")
		assert.Equal(t, payload, readPayload, "Payload mismatch")
	})

	t.Run("Successful Roundtrip with Empty Payload", func(t *testing.T) {
		var buf bytes.Buffer
		var payload []byte // nil payload
		cmdType := CommandQueryEnd

		err := WriteFrame(&buf, cmdType, payload)
		assert.NoError(t, err)

		readCmdType, readPayload, err := ReadFrame(&buf)
		assert.NoError(t, err)

		assert.Equal(t, cmdType, readCmdType)
		assert.Empty(t, readPayload, "Payload should be empty")
	})

	t.Run("Corrupted Data", func(t *testing.T) {
		var buf bytes.Buffer
		payload := []byte("this data will be corrupted")
		WriteFrame(&buf, CommandQuery, payload)

		// Corrupt the data by flipping a bit in the payload
		corruptedBytes := buf.Bytes()
		corruptedBytes[10] ^= 0xff // Flip a bit

		_, _, err := ReadFrame(bytes.NewReader(corruptedBytes))
		assert.Error(t, err, "Expected an error for corrupted data")

		var em *ErrorMessage
		if assert.ErrorAs(t, err, &em) {
			assert.Contains(t, em.Message, ErrChecksumMismatch.Error(), "Error message should indicate a checksum mismatch")
		}
	})
}

func TestQueryRequest_EncodeDecode(t *testing.T) {
	testCases := []struct {
		name    string
		request QueryRequest
	}{
		{
			name:    "simple query string",
			request: QueryRequest{QueryString: `QUERY cpu.usage FROM 1672531200 TO 1672534800;`},
		},
		{
			name:    "empty query string",
			request: QueryRequest{QueryString: ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			if err := EncodeQueryRequest(&buf, tc.request); err != nil {
				t.Fatalf("EncodeQueryRequest failed: %v", err)
			}

			if decodedRequest, err := DecodeQueryRequest(&buf); err != nil {
				t.Fatalf("DecodeQueryRequest failed: %v", err)
			} else if !reflect.DeepEqual(tc.request, decodedRequest) {
				t.Errorf("Decoded request does not match original.\nOriginal: %+v\nDecoded:  %+v", tc.request, decodedRequest)
			}
		})
	}
}

func TestQueryResponse_EncodeDecode(t *testing.T) {
	testCases := []struct {
		name     string
		response QueryResponse
	}{
		{
			name: "response with multiple points and sequence id",
			response: QueryResponse{
				Status: ResponseDataRow,
				Flags:  PointItemFlagWithSequenceID,
				Results: []QueryResultLine{
					{
						SequenceID: 101,
						Metric:     "cpu.usage",
						Tags:       map[string]string{"host": "server-a"},
						Timestamp:  1672531200,
						Fields:     mustNewFieldValues(t, map[string]interface{}{"value": 10.5}),
					},
					{
						SequenceID: 102,
						Metric:     "cpu.usage",
						Tags:       map[string]string{"host": "server-b"},
						Timestamp:  1672531201,
						Fields:     mustNewFieldValues(t, map[string]interface{}{"value": 11.5}),
					},
				},
			},
		},
		{
			name: "response with single point without sequence id",
			response: QueryResponse{
				Status: ResponseDataRow,
				Results: []QueryResultLine{
					{
						// SequenceID is 0 by default
						Metric:    "cpu.usage",
						Timestamp: 1672531300,
						Fields:    mustNewFieldValues(t, map[string]interface{}{"value": 12.5}),
					},
				},
			},
		},
		{
			name: "response with no data points",
			response: QueryResponse{
				Status:  ResponseDataEnd,
				Flags:   0,
				Results: nil, // Canonical empty slice
			},
		},
		{
			name: "response with aggregated flag",
			response: QueryResponse{
				Status: ResponseDataRow,
				Flags:  PointItemFlagIsAggregated,
				Results: []QueryResultLine{
					{
						Timestamp: 1672531000,
						AggregatedValues: map[string]float64{
							"count_value": 10,
							"sum_value":   50,
							"avg_value":   5,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			if err := EncodeQueryResponse(&buf, tc.response); err != nil {
				t.Fatalf("EncodeQueryResponse failed: %v", err)
			}

			if decodedResponse, err := DecodeQueryResponse(&buf); err != nil {
				t.Fatalf("DecodeQueryResponse failed: %v", err)
			} else if !reflect.DeepEqual(tc.response, decodedResponse) {
				t.Errorf("Decoded response does not match original.\nOriginal: %+v\nDecoded:  %+v", tc.response, decodedResponse)
			}
		})
	}
}

func TestManipulateResponse_EncodeDecode(t *testing.T) {
	testCases := []struct {
		name     string
		response ManipulateResponse
	}{
		{
			name: "response with multiple sequence ids",
			response: ManipulateResponse{
				Status:       ResponseOK,
				RowsAffected: 2,
				SequenceID:   []uint64{101, 102},
			},
		},
		{
			name: "response with no sequence ids",
			response: ManipulateResponse{
				Status:       ResponseOK,
				RowsAffected: 0,
				SequenceID:   nil,
			},
		},
		{
			name: "error response with no sequence ids",
			response: ManipulateResponse{
				Status:       ResponseError,
				RowsAffected: 0,
				SequenceID:   nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Encode
			if err := EncodeManipulateResponse(&buf, tc.response); err != nil {
				t.Fatalf("EncodeManipulateResponse failed: %v", err)
			}

			// Decode
			decodedResponse, err := DecodeManipulateResponse(&buf)
			if err != nil {
				t.Fatalf("DecodeManipulateResponse failed: %v", err)
			}

			// Compare
			if !reflect.DeepEqual(tc.response, decodedResponse) {
				t.Errorf("Decoded response does not match original.\nOriginal: %+v\nDecoded:  %+v", tc.response, decodedResponse)
			}
		})
	}
}
