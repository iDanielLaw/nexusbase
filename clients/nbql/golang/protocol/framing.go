package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// MaxFrameSize is the maximum size of a frame's payload.
	// This is a safety measure to prevent allocating huge amounts of memory for a single frame.
	MaxFrameSize = 128 * 1024 * 1024 // 128 MB
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// WriteFrame writes a command type and payload to the writer.
// It prepends the payload with a 4-byte length prefix.
// This implementation mirrors the server's WriteFrame for consistency and efficiency.
func WriteFrame(w io.Writer, cmdType CommandType, payload []byte) error {
	hasher := crc32.New(crc32cTable)

	// Use a MultiWriter to write to both the output writer and the hasher simultaneously.
	multi := io.MultiWriter(w, hasher)

	// Write header to both writer and hasher
	// 1. Command Type (1 byte)
	if err := binary.Write(multi, binary.BigEndian, cmdType); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	// 2. Payload Length (4 bytes) - length includes the 4-byte checksum
	if err := binary.Write(multi, binary.BigEndian, uint32(len(payload)+4)); err != nil {
		return fmt.Errorf("failed to write payload length: %w", err)
	}

	// Write payload to both writer and hasher
	if len(payload) > 0 {
		if _, err := multi.Write(payload); err != nil {
			return fmt.Errorf("failed to write payload: %w", err)
		}
	}

	// Get the checksum of (cmdType + length + payload) and write it to the original writer.
	checksum := hasher.Sum32()
	if err := binary.Write(w, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}

// ReadFrame reads a command type and payload from the reader.
func ReadFrame(r *bufio.Reader) (CommandType, []byte, error) {
	cmdTypeByte, err := r.ReadByte()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read command type: %w", err)
	}

	lenBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBytes); err != nil {
		return 0, nil, fmt.Errorf("failed to read payload length: %w", err)
	}
	payloadLen := binary.BigEndian.Uint32(lenBytes)

	if payloadLen < 4 {
		return 0, nil, fmt.Errorf("invalid payload length: %d", payloadLen)
	}

	if payloadLen > MaxFrameSize {
		return 0, nil, fmt.Errorf("frame size %d exceeds maximum %d", payloadLen, MaxFrameSize)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("failed to read payload: %w", err)
	}

	// Check Valid Checksum CRC32
	hasher := crc32.New(crc32cTable)
	hasher.Write(append([]byte{cmdTypeByte}, lenBytes...))
	hasher.Write(payload[:len(payload)-4])

	checksum := binary.BigEndian.Uint32(payload[len(payload)-4:])
	if checksum != hasher.Sum32() {
		return 0, nil, fmt.Errorf("checksum mismatch")
	}
	return CommandType(cmdTypeByte), payload[:len(payload)-4], nil
}
