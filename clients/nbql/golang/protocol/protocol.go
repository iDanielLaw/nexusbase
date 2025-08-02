package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/INLOpen/nexusbase/clients/nbql/golang/core"
)

func EncodePushRequest(w io.Writer, req PushRequest) error {
	// Metric
	if err := writeStringWithLength(w, req.Metric); err != nil {
		return err
	}
	// Tags
	if err := writeStringMap(w, req.Tags); err != nil {
		return err
	}
	// Timestamp
	if err := binary.Write(w, binary.BigEndian, req.Timestamp); err != nil {
		return err
	}
	// Fields
	fieldBytes, err := req.Fields.MarshalBinary()
	if err != nil {
		return err
	}
	return writeBytesWithLength(w, fieldBytes)
}

func DecodePushRequest(r io.Reader) (PushRequest, error) {
	var req PushRequest
	var err error

	req.Metric, err = readStringWithLength(r)
	if err != nil {
		return req, err
	}
	req.Tags, err = readStringMap(r)
	if err != nil {
		return req, err
	}
	if err := binary.Read(r, binary.BigEndian, &req.Timestamp); err != nil {
		return req, err
	}
	fieldBytes, err := readBytesWithLength(r)
	if err != nil {
		return req, err
	}
	req.Fields, err = core.DecodeFields(bytes.NewReader(fieldBytes))
	return req, err
}

func EncodePushsRequest(w io.Writer, req PushsRequest) error {
	// Number of items
	if err := binary.Write(w, binary.BigEndian, uint32(len(req.Items))); err != nil {
		return err
	}
	for _, item := range req.Items {
		// Metric
		if err := writeStringWithLength(w, item.Metric); err != nil {
			return err
		}
		// Tags
		if err := writeStringMap(w, item.Tags); err != nil {
			return err
		}
		// Timestamp
		if err := binary.Write(w, binary.BigEndian, item.Timestamp); err != nil {
			return err
		}
		// Fields
		fieldBytes, err := item.Fields.MarshalBinary()
		if err != nil {
			return err
		}
		if err := writeBytesWithLength(w, fieldBytes); err != nil {
			return err
		}
	}
	return nil
}

func DecodePushsRequest(r io.Reader) (PushsRequest, error) {
	var req PushsRequest
	var numItems uint32
	if err := binary.Read(r, binary.BigEndian, &numItems); err != nil {
		return req, err
	}
	req.Items = make([]PushItem, numItems)
	for i := range req.Items {
		var err error
		req.Items[i].Metric, err = readStringWithLength(r)
		if err != nil {
			return req, err
		}
		req.Items[i].Tags, err = readStringMap(r)
		if err != nil {
			return req, err
		}
		if err := binary.Read(r, binary.BigEndian, &req.Items[i].Timestamp); err != nil {
			return req, err
		}
		fieldBytes, err := readBytesWithLength(r)
		if err != nil {
			return req, err
		}
		req.Items[i].Fields, err = core.DecodeFields(bytes.NewReader(fieldBytes))
		if err != nil {
			return req, err
		}
	}
	return req, nil
}

func EncodeQueryRequest(w io.Writer, req QueryRequest) error {
	return writeStringWithLength(w, req.QueryString)
}

func DecodeQueryRequest(r io.Reader) (QueryRequest, error) {
	var req QueryRequest
	var err error
	req.QueryString, err = readStringWithLength(r)
	return req, err
}

func EncodeManipulateResponse(w io.Writer, resp ManipulateResponse) error {
	return binary.Write(w, binary.BigEndian, resp.RowsAffected)
}

func DecodeManipulateResponse(r io.Reader) (ManipulateResponse, error) {
	var resp ManipulateResponse
	err := binary.Read(r, binary.BigEndian, &resp.RowsAffected)
	return resp, err
}

func EncodeQueryResponse(w io.Writer, resp QueryResponse) error {
	if err := binary.Write(w, binary.BigEndian, resp.Status); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, resp.Flags); err != nil {
		return err
	}
	if err := writeStringWithLength(w, resp.NextCursor); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(resp.Results))); err != nil {
		return err
	}
	for _, line := range resp.Results {
		if err := binary.Write(w, binary.BigEndian, line.SequenceID); err != nil {
			return err
		}
		if err := writeStringMap(w, line.Tags); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, line.Timestamp); err != nil {
			return err
		}
		fieldBytes, err := line.Fields.MarshalBinary()
		if err != nil {
			return err
		}
		if err := writeBytesWithLength(w, fieldBytes); err != nil {
			return err
		}
		if err := writeFloat64Map(w, line.AggregatedValues); err != nil {
			return err
		}
	}
	return nil
}

func DecodeQueryResponse(r io.Reader) (QueryResponse, error) {
	var resp QueryResponse
	if err := binary.Read(r, binary.BigEndian, &resp.Status); err != nil {
		return resp, fmt.Errorf("failed to read status: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &resp.Flags); err != nil {
		return resp, fmt.Errorf("failed to read flags: %w", err)
	}
	
	var err error
	resp.NextCursor, err = readStringWithLength(r)
	if err != nil {
		return resp, fmt.Errorf("failed to read cursor: %w", err)
	}

	var numResults uint32
	if err := binary.Read(r, binary.BigEndian, &numResults); err != nil {
		return resp, fmt.Errorf("failed to read number of results: %w", err)
	}
	resp.Results = make([]QueryResultLine, numResults)
	for i := range resp.Results {
		if err := binary.Read(r, binary.BigEndian, &resp.Results[i].SequenceID); err != nil {
			return resp, err
		}

		var err error
		resp.Results[i].Tags, err = readStringMap(r)
		if err != nil {
			return resp, err
		}
		if err := binary.Read(r, binary.BigEndian, &resp.Results[i].Timestamp); err != nil {
			return resp, err
		}

		if resp.Flags & PointItemFlagIsAggregated != 0 {
			resp.Results[i].AggregatedValues, err = readFloat64Map(r)
			if err != nil {
				return resp, err
			}
		} else {
			// For non-aggregated data, fields are sent as a length-prefixed byte slice.
			fieldBytes, err := readBytesWithLength(r)
			if err != nil {
				return resp, fmt.Errorf("failed to read field bytes for item %d: %w", i, err)
			}
			resp.Results[i].Fields, err = core.DecodeFields(bytes.NewReader(fieldBytes))
			if err != nil {
				return resp, fmt.Errorf("failed to decode fields for item %d: %w", i, err)
			}
		}
	}
	return resp, nil
}

func EncodeQueryEndResponse(w io.Writer, resp QueryEndResponse) error {
	if err := binary.Write(w, binary.BigEndian, resp.Status); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, resp.TotalRows)
}

func DecodeQueryEndResponse(r io.Reader) (QueryEndResponse, error) {
	var resp QueryEndResponse
	if err := binary.Read(r, binary.BigEndian, &resp.Status); err != nil {
		return resp, err
	}
	err := binary.Read(r, binary.BigEndian, &resp.TotalRows)
	return resp, err
}

func EncodeErrorMessage(w io.Writer, msg *ErrorMessage) error {
	return writeStringWithLength(w, msg.Message)
}

func DecodeErrorMessage(r io.Reader) error {
	msg, err := readStringWithLength(r)
	if err != nil {
		return fmt.Errorf("failed to decode error message: %w", err)
	}
	return fmt.Errorf(msg)
}
