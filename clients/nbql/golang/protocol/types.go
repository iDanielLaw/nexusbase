package protocol

import (
	"encoding/binary"
	"io"

	"github.com/INLOpen/nexusbase/clients/nbql/golang/core"
)

type CommandType byte

const (
	CommandPush            CommandType = 0x01
	CommandPushs           CommandType = 0x02
	CommandQuery           CommandType = 0x10
	CommandManipulate      CommandType = 0x20
	CommandQueryResultPart CommandType = 0x11
	CommandQueryEnd        CommandType = 0x12
	CommandError           CommandType = 0xEE
)

type ResponseStatus byte

const (
	ResponseDataRow ResponseStatus = 0x10
	ResponseDataEnd ResponseStatus = 0x11
)

type PointItemFlag uint8

const (
	PointItemFlagIsAggregated PointItemFlag = 0x02
)

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

type QueryRequest struct {
	QueryString string
}

type ManipulateResponse struct {
	RowsAffected uint64
}

type QueryResultLine struct {
	SequenceID       uint64
	Tags             map[string]string
	Timestamp        int64
	Fields           core.FieldValues
	AggregatedValues map[string]float64
}

type QueryResponse struct {
	Status     ResponseStatus
	Flags      PointItemFlag
	Results    []QueryResultLine
	NextCursor string
}

type QueryEndResponse struct {
	Status    ResponseStatus
	TotalRows uint64
}

type ErrorMessage struct {
	Message string
}

func writeStringWithLength(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func readStringWithLength(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeBytesWithLength(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readBytesWithLength(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeStringMap(w io.Writer, m map[string]string) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := writeStringWithLength(w, k); err != nil {
			return err
		}
		if err := writeStringWithLength(w, v); err != nil {
			return err
		}
	}
	return nil
}

func readStringMap(r io.Reader) (map[string]string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	m := make(map[string]string, length)
	for i := 0; i < int(length); i++ {
		k, err := readStringWithLength(r)
		if err != nil {
			return nil, err
		}
		v, err := readStringWithLength(r)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

func writeFloat64Map(w io.Writer, m map[string]float64) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := writeStringWithLength(w, k); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return err
		}
	}
	return nil
}

func readFloat64Map(r io.Reader) (map[string]float64, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	m := make(map[string]float64, length)
	for i := 0; i < int(length); i++ {
		k, err := readStringWithLength(r)
		if err != nil {
			return nil, err
		}
		var v float64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
