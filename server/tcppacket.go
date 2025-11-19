package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ConnectOp byte
type ResponseOp byte

const (
	ConnectRequestAuthenticationOp  ConnectOp = 1
	ConnectResponseAuthenticationOp ConnectOp = 100
)

const (
	ResponseOK    ResponseOp = 1
	ResponseError ResponseOp = 2
)

type IPacket interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}

type AuthenticationPacket struct {
	Version byte
	Op      ConnectOp
	Payload IPacket
}

type RequestAuthenticationPacket struct {
	Username string
	Password string
}

type ResponseAuthenticationPacket struct {
	Status  ResponseOp
	Message string
}

func (a *AuthenticationPacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	// 1. เขียน Version และ Op
	buf.WriteByte(a.Version)
	buf.WriteByte(byte(a.Op))

	// 2. Marshal payload
	var payload []byte
	var err error
	if a.Payload != nil {
		payload, err = a.Payload.MarshalBinary()
		if err != nil {
			return nil, err
		}
	}

	// 3. เขียนความยาวของ payload (2 bytes)
	lenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBytes, uint16(len(payload)))
	buf.Write(lenBytes)

	// 4. เขียนข้อมูล payload
	buf.Write(payload)
	return buf.Bytes(), nil
}

func (a *AuthenticationPacket) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("authentication packet data too short for header: got %d bytes, want at least 4", len(data))
	}
	a.Version = data[0]
	a.Op = ConnectOp(data[1])
	payloadLen := binary.BigEndian.Uint16(data[2:4])

	if payloadLen == 0 {
		if a.Payload != nil {
			return a.Payload.UnmarshalBinary([]byte{})
		}
		return nil
	}

	if a.Payload == nil {
		return fmt.Errorf("cannot unmarshal non-empty payload into a nil IPacket")
	}

	expectedLen := 4 + int(payloadLen)
	if len(data) < expectedLen {
		return fmt.Errorf("authentication packet data too short for payload: got %d bytes, want %d", len(data), expectedLen)
	}

	return a.Payload.UnmarshalBinary(data[4 : 4+payloadLen])
}

func (r *RequestAuthenticationPacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	lenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBytes, uint16(len(r.Username)))
	buf.Write(lenBytes)
	buf.WriteString(r.Username)
	binary.BigEndian.PutUint16(lenBytes, uint16(len(r.Password)))
	buf.Write(lenBytes)
	buf.WriteString(r.Password)
	return buf.Bytes(), nil
}

func (r *RequestAuthenticationPacket) UnmarshalBinary(data []byte) error {
	offset := 0
	if len(data) < offset+2 {
		return fmt.Errorf("data too short for username length")
	}
	usernameLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if len(data) < offset+usernameLen {
		return fmt.Errorf("data too short for username")
	}
	r.Username = string(data[offset : offset+usernameLen])
	offset += usernameLen

	if len(data) < offset+2 {
		return fmt.Errorf("data too short for password length")
	}
	passwordLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if len(data) < offset+passwordLen {
		return fmt.Errorf("data too short for password")
	}
	r.Password = string(data[offset : offset+passwordLen])

	return nil
}

func (res *ResponseAuthenticationPacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(res.Status))
	lenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBytes, uint16(len(res.Message)))
	buf.Write(lenBytes)
	buf.WriteString(res.Message)
	return buf.Bytes(), nil
}

func (res *ResponseAuthenticationPacket) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("data too short for status")
	}
	res.Status = ResponseOp(data[0])

	if len(data) < 3 {
		return fmt.Errorf("data too short for message length")
	}
	messageLen := int(binary.BigEndian.Uint16(data[1:3]))

	if len(data) < 3+messageLen {
		return fmt.Errorf("data too short for message content")
	}
	res.Message = string(data[3 : 3+messageLen])
	return nil
}
