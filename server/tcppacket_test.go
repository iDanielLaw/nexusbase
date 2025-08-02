package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticationPacket_MarshalUnmarshalBinary(t *testing.T) {
	testCases := []struct {
		name          string
		packet        *AuthenticationPacket
		unmarshalInto *AuthenticationPacket // object ที่จะใช้รับข้อมูลที่ unmarshal
	}{
		{
			name: "With Payload",
			packet: &AuthenticationPacket{
				Version: 1,
				Op:      ConnectRequestAuthenticationOp,
				Payload: &MockPacket{Data: []byte("hello world")},
			},
			unmarshalInto: &AuthenticationPacket{
				Payload: &MockPacket{}, // ต้องสร้าง instance ของ payload ไว้ก่อน
			},
		},
		{
			name: "With Empty Payload",
			packet: &AuthenticationPacket{
				Version: 2,
				Op:      ConnectResponseAuthenticationOp,
				Payload: &MockPacket{Data: []byte{}},
			},
			unmarshalInto: &AuthenticationPacket{
				Payload: &MockPacket{}, // ต้องสร้าง instance ของ payload ไว้ก่อน
			},
		},
		{
			name: "With Nil Payload",
			packet: &AuthenticationPacket{
				Version: 3,
				Op:      ConnectRequestAuthenticationOp,
				Payload: nil,
			},
			unmarshalInto: &AuthenticationPacket{
				// ไม่ต้องสร้าง instance ของ payload
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal packet ต้นฉบับ
			marshaledData, err := tc.packet.MarshalBinary()
			require.NoError(t, err)

			// Unmarshal ข้อมูลลงใน packet ใหม่
			newPacket := tc.unmarshalInto
			newPacket.UnmarshalBinary(marshaledData)

			// Assertions
			assert.Equal(t, tc.packet.Version, newPacket.Version)
			assert.Equal(t, tc.packet.Op, newPacket.Op)
			assert.Equal(t, tc.packet.Payload, newPacket.Payload, "Payloads should be equal")
		})
	}
}

func TestAuthenticationPacket_UnmarshalBinary_Errors(t *testing.T) {
	t.Run("Error on data too short for header", func(t *testing.T) {
		packet := &AuthenticationPacket{}
		err := packet.UnmarshalBinary([]byte{1, 2}) // น้อยกว่า 4 bytes
		assert.Error(t, err)
		assert.ErrorContains(t, err, "too short for header")
	})

	t.Run("Error on data too short for payload", func(t *testing.T) {
		packet := &AuthenticationPacket{Payload: &MockPacket{}}
		// Header บอกว่า payload มี 10 bytes แต่เราส่งไปแค่ 5
		data := []byte{1, 1, 0, 10, 'h', 'e', 'l', 'l', 'o'}
		err := packet.UnmarshalBinary(data)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "too short for payload")
	})

	t.Run("Error on unmarshalling into nil payload", func(t *testing.T) {
		packet := &AuthenticationPacket{Payload: nil} // payload ปลายทางเป็น nil
		// ข้อมูลมี payload ที่ความยาวไม่เป็นศูนย์
		data := []byte{1, 1, 0, 5, 'h', 'e', 'l', 'l', 'o'}
		err := packet.UnmarshalBinary(data)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot unmarshal non-empty payload")
	})
}

func TestRequestAuthenticationPacket_MarshalUnmarshalBinary(t *testing.T) {
	testCases := []struct {
		name   string
		packet *RequestAuthenticationPacket
	}{
		{
			name: "Standard credentials",
			packet: &RequestAuthenticationPacket{
				Username: "testuser",
				Password: "password123",
			},
		},
		{
			name: "Empty credentials",
			packet: &RequestAuthenticationPacket{
				Username: "",
				Password: "",
			},
		},
		{
			name: "With unicode characters",
			packet: &RequestAuthenticationPacket{
				Username: "ผู้ใช้ทดสอบ",
				Password: "รหัสผ่านลับสุดยอด",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.packet.MarshalBinary()
			require.NoError(t, err)

			newPacket := &RequestAuthenticationPacket{}
			err = newPacket.UnmarshalBinary(data)
			require.NoError(t, err)

			assert.Equal(t, tc.packet, newPacket)
		})
	}
}

func TestRequestAuthenticationPacket_UnmarshalBinary_Errors(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expectedErr string
	}{
		{"data too short for username length", []byte{0}, "username length"},
		{"data too short for username content", []byte{0, 5, 'u', 's'}, "for username"},
		{"data too short for password length", []byte{0, 4, 'u', 's', 'e', 'r', 0}, "password length"},
		{"data too short for password content", []byte{0, 4, 'u', 's', 'e', 'r', 0, 8, 'p', 'a', 's', 's'}, "for password"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			packet := &RequestAuthenticationPacket{}
			err := packet.UnmarshalBinary(tc.data)
			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

func TestResponseAuthenticationPacket_MarshalUnmarshalBinary(t *testing.T) {
	testCases := []struct {
		name   string
		packet *ResponseAuthenticationPacket
	}{
		{"OK response with message", &ResponseAuthenticationPacket{Status: ResponseOK, Message: "Authentication successful"}},
		{"Error response with message", &ResponseAuthenticationPacket{Status: ResponseError, Message: "Invalid username or password"}},
		{"Response with empty message", &ResponseAuthenticationPacket{Status: ResponseOK, Message: ""}},
		{"Response with unicode message", &ResponseAuthenticationPacket{Status: ResponseOK, Message: "ยืนยันตัวตนสำเร็จ"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.packet.MarshalBinary()
			require.NoError(t, err)

			newPacket := &ResponseAuthenticationPacket{}
			err = newPacket.UnmarshalBinary(data)
			require.NoError(t, err)

			assert.Equal(t, tc.packet, newPacket)
		})
	}
}

func TestResponseAuthenticationPacket_UnmarshalBinary_Errors(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expectedErr string
	}{
		{"data too short for status", []byte{}, "for status"},
		{"data too short for message length", []byte{1, 0}, "message length"},
		{"data too short for message content", []byte{1, 0, 5, 'O', 'K'}, "message content"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			packet := &ResponseAuthenticationPacket{}
			err := packet.UnmarshalBinary(tc.data)
			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
