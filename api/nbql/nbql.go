package nbql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/INLOpen/nexusbase/core"
)

// CommandType defines the type of command in a binary request.
type CommandType byte

type NBQLResponse interface {
	isNBResponse()
}

const (
	// CommandPush stores a single data point.
	CommandPush  CommandType = 0x01
	CommandPushs CommandType = 0x02
	// CommandQuery retrieves data.
	CommandQuery CommandType = 0x10
	// ... other command types can be added here ...
	CommandManipulate CommandType = 0x20

	// CommandQueryResultPart sends a single chunk of query results.
	CommandQueryResultPart CommandType = 0x11
	CommandQueryEnd        CommandType = 0x12

	CommandError CommandType = 0xEE
)

// ResponseType defines the type of response from the server.
type ResponseType byte

const (
	// ResponseOK indicates a successful operation with a message.
	ResponseOK ResponseType = 0x00
	// ResponseError indicates an error occurred.
	ResponseError ResponseType = 0x01
	// ResponseDataRow sends a single row of query results.
	ResponseDataRow ResponseType = 0x10
	// ResponseDataEnd signals the end of a query result stream.
	ResponseDataEnd ResponseType = 0x11
)

type PointItemFlag byte

const (
	PointItemFlagWithSequenceID PointItemFlag = 0x01
	PointItemFlagIsAggregated   PointItemFlag = 0x02
	PointItemFlagHasFields      PointItemFlag = 0x04
)

const (
	// headerSize is the size of the command/response header (Type + PayloadLength).
	headerSize = 1 + 4 // 5 bytes (Type + Length)
	crcSize    = 4     // 4 bytes for CRC32 checksum
)

var (
	// crc32cTable is a pre-calculated table for the Castagnoli polynomial, used for CRC-32C checksums.
	crc32cTable         = crc32.MakeTable(crc32.Castagnoli)
	ErrChecksumMismatch = errors.New("nbsql: frame checksum mismatch")
)

// Header represents the common header for all binary messages.
type Header struct {
	Type   byte
	Length uint32
}

type ErrorMessage struct {
	Code    uint16
	Message string
}

func (e *ErrorMessage) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

// PushRequest represents the payload for a PUSH command.
type PushRequest struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    core.FieldValues
}

type PushItem struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    core.FieldValues
}

type PushsRequest struct {
	Items []PushItem
}

// ManipulateResponse represents the playload for PUSH, PUSHS, REMOVE command.
type ManipulateResponse struct {
	Status       ResponseType
	RowsAffected uint64
	SequenceID   []uint64
}

// QueryEndResponse is sent after all QueryResultPart frames have been sent.
type QueryEndResponse struct {
	Status    ResponseType
	TotalRows uint64
	Message   string
}

func (r ManipulateResponse) isNBResponse() {}

// QueryRequest represents the payload for a QUERY command.
type QueryRequest struct {
	QueryString string
	Params      map[string]string
}

type QueryResultLine struct {
	SequenceID       uint64
	Metric           string
	Tags             map[string]string
	Timestamp        int64
	Fields           core.FieldValues
	IsAggregated     bool
	WindowStartTime  int64
	AggregatedValues map[string]float64
}

type QueryResponse struct {
	Status     ResponseType
	Flags      PointItemFlag
	Results    []QueryResultLine
	NextCursor string
}

func (r QueryResponse) isNBResponse() {}

func makeError(code uint16, err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return err
	}

	return &ErrorMessage{
		Code:    code,
		Message: err.Error(),
	}
}

// --- Encoding Functions ---

// writeString writes a length-prefixed string to the writer.
func writeString(w io.Writer, s string) error {
	b := []byte(s)
	// Write length as uint16
	if err := binary.Write(w, binary.BigEndian, uint16(len(b))); err != nil {
		return err
	}
	// Write string bytes
	if len(b) > 0 {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// writeTags writes a map of tags to the writer.
func writeTags(w io.Writer, tags map[string]string) error {
	// Write tag count as uint16
	if err := binary.Write(w, binary.BigEndian, uint16(len(tags))); err != nil {
		return err
	}
	for k, v := range tags {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeString(w, v); err != nil {
			return err
		}
	}
	return nil
}

// writeAggregatedValues writes a map of aggregate to the writer.
func writeAggregatedValues(w io.Writer, values map[string]float64) error {
	// Write tag count as uint16
	if err := binary.Write(w, binary.BigEndian, uint16(len(values))); err != nil {
		return err
	}
	for k, v := range values {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return err
		}
	}
	return nil
}

// writeFields writes a map of fields to the writer.
func writeFields(w io.Writer, fields core.FieldValues) error {
	data, err := fields.Encode()
	if err != nil {
		return err
	}
	// Write length of encoded fields
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	// Write encoded fields
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// writePointItems a map of Point to the writer.
func writePointItems(w io.Writer, points []QueryResultLine, flag PointItemFlag) error {
	// Write tag count as uint32
	if err := binary.Write(w, binary.BigEndian, uint32(len(points))); err != nil {
		return err
	}
	for _, point := range points {

		if err := binary.Write(w, binary.BigEndian, point.SequenceID); err != nil {
			return err
		}

		if err := writeString(w, point.Metric); err != nil {
			return err
		}
		if err := writeTags(w, point.Tags); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, point.Timestamp); err != nil {
			return err
		}

		// Handle different value types based on flags
		if flag&PointItemFlagIsAggregated != 0 {
			// Write WindowStartTime for aggregated points
			if err := binary.Write(w, binary.BigEndian, point.WindowStartTime); err != nil {
				return err
			}
			if err := writeAggregatedValues(w, point.AggregatedValues); err != nil {
				return err
			}
		} else {
			if err := writeFields(w, point.Fields); err != nil {
				return err
			}
		}

	}

	return nil
}

// EncodeQueryResponse serializes a QueryResponse into a writer.
func EncodeQueryResponse(w io.Writer, resp QueryResponse) error {
	if err := binary.Write(w, binary.BigEndian, resp.Status); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, resp.Flags); err != nil {
		return err
	}

	if err := writeString(w, resp.NextCursor); err != nil {
		return err
	}

	return writePointItems(w, resp.Results, resp.Flags)
}

// EncodePushRequest serializes a PushRequest into a writer.
func EncodePushRequest(w io.Writer, req PushRequest) error {
	// Write metric
	if err := writeString(w, req.Metric); err != nil {
		return makeError(1, fmt.Errorf("failed to write metric: %w", err))
	}
	// Write tags
	if err := writeTags(w, req.Tags); err != nil {
		return makeError(2, fmt.Errorf("failed to write tags: %w", err))
	}
	// Write timestamp
	if err := binary.Write(w, binary.BigEndian, req.Timestamp); err != nil {
		return makeError(3, fmt.Errorf("failed to write timestamp: %w", err))
	}
	// Write fields
	encodedFields, err := req.Fields.Encode()
	if err != nil {
		return makeError(4, fmt.Errorf("failed to encode fields: %w", err))
	}
	// Write length of encoded fields
	if err := binary.Write(w, binary.BigEndian, uint32(len(encodedFields))); err != nil {
		return makeError(4, fmt.Errorf("failed to write fields length: %w", err))
	}
	// Write encoded fields
	if _, err := w.Write(encodedFields); err != nil {
		return makeError(4, fmt.Errorf("failed to write fields data: %w", err))
	}

	return nil
}

// EncodePushsRequest serializes a PushsRequest into a writer.
func EncodePushsRequest(w io.Writer, req PushsRequest) error {
	// Write item count
	if err := binary.Write(w, binary.BigEndian, uint32(len(req.Items))); err != nil {
		return makeError(5, fmt.Errorf("failed to write item count: %w", err))
	}
	// Write items
	for _, item := range req.Items {
		if err := writeString(w, item.Metric); err != nil {
			return makeError(5, fmt.Errorf("failed to write metric: %w", err))
		}
		if err := writeTags(w, item.Tags); err != nil {
			return makeError(5, fmt.Errorf("failed to write tags: %w", err))
		}
		if err := binary.Write(w, binary.BigEndian, item.Timestamp); err != nil {
			return makeError(5, fmt.Errorf("failed to write timestamp: %w", err))
		}
		encodedFields, err := item.Fields.Encode()
		if err != nil {
			return makeError(5, fmt.Errorf("failed to encode fields for item: %w", err))
		}
		if err := binary.Write(w, binary.BigEndian, uint32(len(encodedFields))); err != nil {
			return makeError(5, fmt.Errorf("failed to write fields length for item: %w", err))
		}
		if _, err := w.Write(encodedFields); err != nil {
			return makeError(5, fmt.Errorf("failed to write fields data for item: %w", err))
		}
	}
	return nil
}

// EncodeQueryRequest serializes a QueryRequest into a writer.
func EncodeQueryRequest(w io.Writer, req QueryRequest) error {
	// The payload is now just the query string itself.
	return writeString(w, req.QueryString)
}

// EncodeManipulateResponse serializes a ManipulateResponse into a writer.
func EncodeManipulateResponse(w io.Writer, resp ManipulateResponse) error {
	// Write Status
	if err := binary.Write(w, binary.BigEndian, resp.Status); err != nil {
		return makeError(15, fmt.Errorf("failed to write status: %w", err))
	}

	// Write Affective
	if err := binary.Write(w, binary.BigEndian, resp.RowsAffected); err != nil {
		return makeError(15, fmt.Errorf("failed to write affective: %w", err))
	}

	// Write array of SequenceID
	if err := binary.Write(w, binary.BigEndian, uint16(len(resp.SequenceID))); err != nil {
		return makeError(15, fmt.Errorf("failed to write sequence id: %w", err))
	}

	if len(resp.SequenceID) > 0 {
		// Write length array
		for _, id := range resp.SequenceID {
			if err := binary.Write(w, binary.BigEndian, id); err != nil {
				return makeError(15, err)
			}
		}
	}

	return nil
}

// EncodeQueryEndResponse serializes a QueryEndResponse into a writer.
func EncodeQueryEndResponse(w io.Writer, resp QueryEndResponse) error {
	if err := binary.Write(w, binary.BigEndian, resp.Status); err != nil {
		return makeError(16, fmt.Errorf("failed to write status: %w", err))
	}
	if err := binary.Write(w, binary.BigEndian, resp.TotalRows); err != nil {
		return makeError(16, fmt.Errorf("failed to write total rows: %w", err))
	}
	if err := writeString(w, resp.Message); err != nil {
		return makeError(16, fmt.Errorf("failed to write message: %w", err))
	}
	return nil
}

// EncodeErrorMessage serializes a ErrorMessage into a writer.
func EncodeErrorMessage(w io.Writer, err *ErrorMessage) error {
	if err := binary.Write(w, binary.BigEndian, err.Code); err != nil {
		return makeError(16, err)
	}
	if err := writeString(w, err.Message); err != nil {
		return makeError(16, err)
	}
	return nil
}

// --- Decoding Functions ---

// readString reads a length-prefixed string from the reader.
func readString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// readTags reads a map of tags from the reader.
func readTags(r io.Reader) (map[string]string, error) {
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	// If count is 0, return a nil map to have a canonical representation for "no tags".
	// This handles both nil and empty maps from the encoding side.
	if count == 0 {
		return nil, nil
	}
	tags := make(map[string]string, count)
	for i := 0; i < int(count); i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readString(r)
		if err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, nil
}

// readAggregatedValues reads a map of Aggregate from the reader.
func readAggregatedValues(r io.Reader) (map[string]float64, error) {
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	values := make(map[string]float64, count)
	for i := 0; i < int(count); i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		var v float64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		values[k] = v
	}
	return values, nil
}

// readFields reads a map of Fields from the reader.
func readFields(r io.Reader) (core.FieldValues, error) {
	var fieldsLen uint32
	if err := binary.Read(r, binary.BigEndian, &fieldsLen); err != nil {
		return nil, fmt.Errorf("failed to read fields length: %w", err)
	}

	if fieldsLen == 0 {
		return nil, nil
	}

	fieldsBytes := make([]byte, fieldsLen)
	if _, err := io.ReadFull(r, fieldsBytes); err != nil {
		return nil, fmt.Errorf("failed to read fields data: %w", err)
	}

	return core.DecodeFieldsFromBytes(fieldsBytes)
}

// readPointItems read a map of point from the reader.
func readPointItems(r io.Reader, flag PointItemFlag) ([]QueryResultLine, error) {
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	results := make([]QueryResultLine, 0, count)
	for i := 0; i < int(count); i++ {
		var item QueryResultLine
		var err error

		//if flag&PointItemFlagWithSequenceID != 0 {
		if err := binary.Read(r, binary.BigEndian, &item.SequenceID); err != nil {
			return nil, fmt.Errorf("failed to read sequence id for item %d: %w", i, err)
		}
		//}

		item.Metric, err = readString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read metric for item %d: %w", i, err)
		}

		item.Tags, err = readTags(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read tags for item %d: %w", i, err)
		}

		if err := binary.Read(r, binary.BigEndian, &item.Timestamp); err != nil {
			return nil, fmt.Errorf("failed to read timestamp for item %d: %w", i, err)
		}

		if flag&PointItemFlagIsAggregated != 0 {
			item.IsAggregated = true
			if err := binary.Read(r, binary.BigEndian, &item.WindowStartTime); err != nil {
				return nil, fmt.Errorf("failed to read window start time for item %d: %w", i, err)
			}
			item.AggregatedValues, err = readAggregatedValues(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read aggregated values for item %d: %w", i, err)
			}
		} else {
			item.IsAggregated = false
			field, err := readFields(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read fields for item %d: %w", i, err)
			}
			item.Fields = field
		}

		if len(item.AggregatedValues) == 0 {
			item.AggregatedValues = nil
		}

		if len(item.Fields) == 0 {
			item.Fields = nil
		}

		results = append(results, item)
	}
	return results, nil
}

// DecodeQueryResponse deserializes a QueryResponse from a reader.
func DecodeQueryResponse(r io.Reader) (QueryResponse, error) {
	var resp QueryResponse
	var err error
	if err := binary.Read(r, binary.BigEndian, &resp.Status); err != nil {
		return QueryResponse{}, makeError(17, fmt.Errorf("failed to read status: %w", err))
	}
	var flag PointItemFlag
	if err := binary.Read(r, binary.BigEndian, &flag); err != nil {
		return QueryResponse{}, makeError(17, fmt.Errorf("failed to read flag: %w", err))
	}
	resp.Flags = flag

	resp.NextCursor, err = readString(r)
	if err != nil {
		return QueryResponse{}, makeError(17, fmt.Errorf("failed to read next cursor: %w", err))
	}

	resp.Results, err = readPointItems(r, resp.Flags)
	if err != nil {
		return QueryResponse{}, makeError(17, fmt.Errorf("failed to read items: %w", err))
	}
	return resp, nil
}

// DecodePushRequest deserializes a PushRequest from a reader.
func DecodePushRequest(r io.Reader) (PushRequest, error) {
	var req PushRequest
	var err error

	req.Metric, err = readString(r)
	if err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to read metric: %w", err))
	}

	req.Tags, err = readTags(r)
	if err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to read tags: %w", err))
	}

	if err := binary.Read(r, binary.BigEndian, &req.Timestamp); err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to read timestamp: %w", err))
	}

	var fieldsLen uint32
	if err := binary.Read(r, binary.BigEndian, &fieldsLen); err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to read fields length: %w", err))
	}

	fieldsBytes := make([]byte, fieldsLen)
	if _, err := io.ReadFull(r, fieldsBytes); err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to read fields data: %w", err))
	}
	req.Fields, err = core.DecodeFieldsFromBytes(fieldsBytes)
	if err != nil {
		return PushRequest{}, makeError(18, fmt.Errorf("failed to decode fields: %w", err))
	}

	if len(req.Fields) == 0 {
		req.Fields = nil
	}

	return req, nil
}

// DecodePushsRequest deserializes a PushsRequest from a reader.
func DecodePushsRequest(r io.Reader) (PushsRequest, error) {
	var req PushsRequest
	var err error

	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return PushsRequest{}, makeError(19, fmt.Errorf("failed to read item count: %w", err))
	}

	req.Items = make([]PushItem, count)
	for i := 0; i < int(count); i++ {
		req.Items[i].Metric, err = readString(r)
		if err != nil {
			return PushsRequest{}, makeError(19, fmt.Errorf("failed to read metric: %w", err))
		}

		req.Items[i].Tags, err = readTags(r)
		if err != nil {
			return PushsRequest{}, makeError(19, fmt.Errorf("failed to read tags for item %d: %w", i, err))
		}
		if err := binary.Read(r, binary.BigEndian, &req.Items[i].Timestamp); err != nil {
			return PushsRequest{}, makeError(19, fmt.Errorf("failed to read timestamp for item %d: %w", i, err))
		}
		var fieldsLen uint32
		if err := binary.Read(r, binary.BigEndian, &fieldsLen); err != nil {
			return PushsRequest{}, makeError(19, fmt.Errorf("failed to read fields length for item %d: %w", i, err))
		}
		if fieldsLen > 0 {
			fieldsBytes := make([]byte, fieldsLen)
			if _, err := io.ReadFull(r, fieldsBytes); err != nil {
				return PushsRequest{}, makeError(19, fmt.Errorf("failed to read fields data for item %d: %w", i, err))
			}
			req.Items[i].Fields, err = core.DecodeFieldsFromBytes(fieldsBytes)
			if err != nil {
				return PushsRequest{}, makeError(19, fmt.Errorf("failed to decode fields for item %d: %w", i, err))
			}
		}
	}
	return req, nil
}

// DecodeQueryRequest deserializes a QueryRequest from a reader.
func DecodeQueryRequest(r io.Reader) (QueryRequest, error) {
	var req QueryRequest
	var err error

	// The payload is now just the query string itself.
	req.QueryString, err = readString(r)
	if err != nil {
		return req, makeError(19, fmt.Errorf("failed to read query string: %w", err))
	}

	return req, nil
}

// DecodeManipulateResponse deserializes a ManipulateResponse from a reader.
func DecodeManipulateResponse(r io.Reader) (ManipulateResponse, error) {
	var resp ManipulateResponse

	// read status
	if err := binary.Read(r, binary.BigEndian, &resp.Status); err != nil {
		return ManipulateResponse{}, makeError(20, err)
	}

	// read Affective
	if err := binary.Read(r, binary.BigEndian, &resp.RowsAffected); err != nil {
		return ManipulateResponse{}, makeError(21, err)
	}

	// read array of sequenceid
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return ManipulateResponse{}, makeError(22, err)
	}

	if count > 0 {
		resp.SequenceID = make([]uint64, 0, count)
		for i := 0; i < int(count); i++ {
			var id uint64
			if err := binary.Read(r, binary.BigEndian, &id); err != nil {
				return ManipulateResponse{}, makeError(23, err)
			}
			resp.SequenceID = append(resp.SequenceID, id)
		}

	}
	return resp, nil
}

// DecodeQueryEndResponse deserializes a QueryEndResponse from a reader.
func DecodeQueryEndResponse(r io.Reader) (QueryEndResponse, error) {
	var resp QueryEndResponse
	var err error

	if err = binary.Read(r, binary.BigEndian, &resp.Status); err != nil {
		return QueryEndResponse{}, makeError(21, err)
	}
	if err = binary.Read(r, binary.BigEndian, &resp.TotalRows); err != nil {
		return QueryEndResponse{}, makeError(21, err)
	}
	resp.Message, err = readString(r)
	if err != nil {
		return QueryEndResponse{}, makeError(21, err)
	}
	return resp, nil
}

// DecodeErrorMessage deserializes a ErrorMessage from a reader.
func DecodeErrorMessage(r io.Reader) error {
	var err ErrorMessage
	if err := binary.Read(r, binary.BigEndian, &err.Code); err != nil {
		return makeError(21, err)
	}
	err.Message, _ = readString(r)
	return &err
}

// --- Frame Handling ---

// WriteFrame writes a complete message frame (header + payload + crc) to the writer.
func WriteFrame(w io.Writer, cmdType CommandType, payload []byte) error {
	hasher := crc32.New(crc32cTable)

	// Use a MultiWriter to write to both the output writer and the hasher simultaneously.
	multi := io.MultiWriter(w, hasher)

	// Write header to both writer and hasher
	if err := binary.Write(multi, binary.BigEndian, cmdType); err != nil {
		return makeError(100, fmt.Errorf("failed to write command type: %w", err))
	}
	if err := binary.Write(multi, binary.BigEndian, uint32(len(payload)+4)); err != nil {
		return makeError(100, fmt.Errorf("failed to write payload length: %w", err))
	}

	// Write payload to both writer and hasher
	if len(payload) > 0 {
		if _, err := multi.Write(payload); err != nil {
			return makeError(100, fmt.Errorf("failed to write payload: %w", err))
		}
	}

	// Get the checksum and write it to the original writer (not the multi-writer).
	checksum := hasher.Sum32()
	if err := binary.Write(w, binary.BigEndian, checksum); err != nil {
		return makeError(100, fmt.Errorf("failed to write checksum: %w", err))
	}

	return nil
}

// ReadFrameHeader reads just the header of a message frame.
func ReadFrameHeader(r io.Reader) (Header, error) {
	var header Header
	var cmdType CommandType
	if err := binary.Read(r, binary.BigEndian, &cmdType); err != nil {
		return Header{}, makeError(200, err)
	}
	header.Type = byte(cmdType)

	if err := binary.Read(r, binary.BigEndian, &header.Length); err != nil {
		return Header{}, makeError(200, err)
	}
	return header, nil
}

// ReadFrame reads a complete message frame, verifies its CRC checksum, and returns the command type and payload.
func ReadFrame(r io.Reader) (CommandType, []byte, error) {
	// 1. Read header
	header, err := ReadFrameHeader(r)
	if err != nil {
		return 0, nil, makeError(201, err)
	}

	// 2. Read payload
	payload := make([]byte, header.Length)
	if header.Length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, makeError(201, err)
		}
	}

	// 3. Read checksum
	receivedChecksum := binary.BigEndian.Uint32(payload[header.Length-4:])

	// 4. Calculate checksum on header + payload
	hasher := crc32.New(crc32cTable)

	// Re-create header bytes for hashing
	headerBytes := make([]byte, headerSize)
	headerBytes[0] = header.Type
	binary.BigEndian.PutUint32(headerBytes[1:5], header.Length)

	hasher.Write(headerBytes)
	hasher.Write(payload[:header.Length-crcSize])
	calculatedChecksum := hasher.Sum32()

	if receivedChecksum != calculatedChecksum {
		return 0, nil, makeError(201, ErrChecksumMismatch)
	}

	return CommandType(header.Type), payload[:header.Length-crcSize], nil
}
