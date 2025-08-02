package auth

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/INLOpen/nexusbase/sys"
	"golang.org/x/crypto/bcrypt"
)

const (
	// UserFileMagic is a magic number to identify the user database file.
	UserFileMagic uint32 = 0x55535244 // "USRD"
	// CurrentUserFileVersion is the current version of the user file format.
	CurrentUserFileVersion uint8 = 1
)

// HashType defines the password hashing algorithm used.
type HashType uint8

const (
	// HashTypeUnknown is an invalid hash type.
	HashTypeUnknown HashType = 0
	// HashTypeBcrypt indicates that bcrypt is used for hashing.
	HashTypeBcrypt HashType = 1
	// HashTypeSHA256 indicates that SHA-256 is used for hashing.
	HashTypeSHA256 HashType = 2
	// HashTypeSHA512 indicates that SHA-512 is used for hashing.
	HashTypeSHA512 HashType = 3
)

// UserFileHeader represents the header of the user database file.
type UserFileHeader struct {
	Magic     uint32
	Version   uint8
	HashType  HashType
	UserCount uint32
}

// UserRecord represents a single user's data within the file.
type UserRecord struct {
	Username     string
	PasswordHash string
	Role         string
}

// WriteUserFile writes a map of users to a binary file at the specified path.
func WriteUserFile(path string, users map[string]UserRecord, hashType HashType) error {
	file, err := sys.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create user file: %w", err)
	}
	defer file.Close()

	header := UserFileHeader{
		Magic:     UserFileMagic,
		Version:   CurrentUserFileVersion,
		HashType:  hashType,
		UserCount: uint32(len(users)),
	}

	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to write user file header: %w", err)
	}

	for _, user := range users {
		if err := writeUserRecord(file, user); err != nil {
			return fmt.Errorf("failed to write user record for '%s': %w", user.Username, err)
		}
	}

	return nil
}

// ReadUserFile reads a binary user file and returns a map of users.
func ReadUserFile(path string) (map[string]UserRecord, HashType, error) {
	file, err := sys.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file doesn't exist, return an empty map, it's not an error.
			// Default to bcrypt for new files.
			return make(map[string]UserRecord), HashTypeBcrypt, nil
		}
		return nil, HashTypeUnknown, fmt.Errorf("failed to open user file: %w", err)
	}
	defer file.Close()

	var header UserFileHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		if err == io.EOF {
			// Empty file, treat as new.
			return make(map[string]UserRecord), HashTypeBcrypt, nil
		}
		return nil, HashTypeUnknown, fmt.Errorf("failed to read user file header: %w", err)
	}

	if header.Magic != UserFileMagic {
		return nil, HashTypeUnknown, fmt.Errorf("invalid user file magic number: got %x", header.Magic)
	}
	if header.Version > CurrentUserFileVersion {
		return nil, HashTypeUnknown, fmt.Errorf("unsupported user file version: got %d", header.Version)
	}
	if header.HashType != HashTypeBcrypt && header.HashType != HashTypeSHA256 && header.HashType != HashTypeSHA512 {
		return nil, HashTypeUnknown, fmt.Errorf("unsupported hash type: got %d", header.HashType)
	}

	users := make(map[string]UserRecord, header.UserCount)
	for i := uint32(0); i < header.UserCount; i++ {
		record, err := readUserRecord(file)
		if err != nil {
			return nil, HashTypeUnknown, fmt.Errorf("failed to read user record #%d: %w", i+1, err)
		}
		users[record.Username] = record
	}

	return users, header.HashType, nil
}

// HashPassword generates a hash for a given password based on the specified hash type.
// NOTE: SHA256/SHA512 are implemented without salting for simplicity. For production use,
// a salted hash mechanism (like Argon2, scrypt, or bcrypt) is strongly recommended.
func HashPassword(password string, hashType HashType) (string, error) {
	switch hashType {
	case HashTypeBcrypt:
		hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return "", err
		}
		return string(hashed), nil
	case HashTypeSHA256:
		h := sha256.New()
		h.Write([]byte(password))
		return hex.EncodeToString(h.Sum(nil)), nil
	case HashTypeSHA512:
		h := sha512.New()
		h.Write([]byte(password))
		return hex.EncodeToString(h.Sum(nil)), nil
	default:
		return "", fmt.Errorf("unsupported hash type: %d", hashType)
	}
}

func writeUserRecord(w io.Writer, user UserRecord) error {
	if err := writeString(w, user.Username); err != nil {
		return err
	}
	if err := writeString(w, user.PasswordHash); err != nil {
		return err
	}
	if err := writeString(w, user.Role); err != nil {
		return err
	}
	return nil
}

func readUserRecord(r io.Reader) (UserRecord, error) {
	var record UserRecord
	var err error

	record.Username, err = readString(r)
	if err != nil {
		return UserRecord{}, err
	}
	record.PasswordHash, err = readString(r)
	if err != nil {
		return UserRecord{}, err
	}
	record.Role, err = readString(r)
	if err != nil {
		return UserRecord{}, err
	}
	return record, nil
}

func writeString(w io.Writer, s string) error {
	data := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint16(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}
