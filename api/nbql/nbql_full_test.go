package nbql

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeAll(t *testing.T) {
	t.Run("PushRequest", func(t *testing.T) {
		req := PushRequest{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "node1"},
			Timestamp: 123,
			Fields:    mustNewFieldValues(t, map[string]interface{}{"v": 1.2}),
		}
		var buf bytes.Buffer
		require.NoError(t, EncodePushRequest(&buf, req))
		out, err := DecodePushRequest(&buf)
		require.NoError(t, err)
		require.Equal(t, req.Metric, out.Metric)
	})
	t.Run("PushsRequest", func(t *testing.T) {
		req := PushsRequest{
			Items: []PushItem{{
				Metric:    "m",
				Tags:      map[string]string{"x": "y"},
				Timestamp: 1,
				Fields:    mustNewFieldValues(t, map[string]interface{}{"a": 1.0}),
			}},
		}
		var buf bytes.Buffer
		require.NoError(t, EncodePushsRequest(&buf, req))
		d, err := DecodePushsRequest(&buf)
		require.NoError(t, err)
		require.Len(t, d.Items, 1)
	})
	t.Run("QueryRequest", func(t *testing.T) {
		req := QueryRequest{QueryString: "QUERY cpu FROM 1 TO 2"}
		var buf bytes.Buffer
		require.NoError(t, EncodeQueryRequest(&buf, req))
		d, err := DecodeQueryRequest(&buf)
		require.NoError(t, err)
		require.Equal(t, req.QueryString, d.QueryString)
	})
	t.Run("ManipulateResponse", func(t *testing.T) {
		r := ManipulateResponse{Status: ResponseOK, RowsAffected: 5, SequenceID: []uint64{1, 2}}
		var buf bytes.Buffer
		require.NoError(t, EncodeManipulateResponse(&buf, r))
		d, err := DecodeManipulateResponse(&buf)
		require.NoError(t, err)
		require.Equal(t, r.RowsAffected, d.RowsAffected)
	})
	t.Run("QueryResponse", func(t *testing.T) {
		r := QueryResponse{
			Status:     ResponseOK,
			Flags:      0,
			NextCursor: base64.StdEncoding.EncodeToString([]byte("c")),
			Results: []QueryResultLine{{
				Metric:    "cpu",
				Tags:      map[string]string{"k": "v"},
				Timestamp: 42,
				Fields:    mustNewFieldValues(t, map[string]interface{}{"v": 2.3}),
			}},
		}
		var buf bytes.Buffer
		require.NoError(t, EncodeQueryResponse(&buf, r))
		d, err := DecodeQueryResponse(&buf)
		require.NoError(t, err)
		require.Equal(t, r.NextCursor, d.NextCursor)
		require.Len(t, d.Results, 1)
	})
	// CRC Frame
	t.Run("CRC Frame", func(t *testing.T) {
		p := []byte("payload")
		var buf bytes.Buffer
		require.NoError(t, WriteFrame(&buf, CommandPush, p))
		c, data, err := ReadFrame(&buf)
		require.NoError(t, err)
		require.Equal(t, CommandPush, c)
		require.Equal(t, p, data)
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("EmptyFrame", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		_, _, err := ReadFrame(buf)
		require.Error(t, err)
	})
	t.Run("InvalidCRC", func(t *testing.T) {
		var buf bytes.Buffer
		raw := []byte("corrupt payload")
		require.NoError(t, WriteFrame(&buf, CommandPush, raw))
		b := buf.Bytes()
		// Flip a byte
		b[len(b)-1] ^= 0xFF
		_, _, err := ReadFrame(bytes.NewReader(b))
		require.Error(t, err)
	})
}
