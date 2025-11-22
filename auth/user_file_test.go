package auth

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/sys"
	"golang.org/x/crypto/bcrypt"
)

func TestUserFile_ReadWrite(t *testing.T) {
	tempDir := t.TempDir()
	userFilePath := filepath.Join(tempDir, "test_users.db")

	testCases := []struct {
		name     string
		hashType HashType
	}{
		{"bcrypt", HashTypeBcrypt},
		{"sha256", HashTypeSHA256},
		{"sha512", HashTypeSHA512},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Write Phase ---
			writerHash, _ := HashPassword("writer_pass", tc.hashType)
			readerHash, _ := HashPassword("reader_pass", tc.hashType)

			usersToWrite := map[string]UserRecord{
				"writer_user": {Username: "writer_user", PasswordHash: writerHash, Role: RoleWriter},
				"reader_user": {Username: "reader_user", PasswordHash: readerHash, Role: RoleReader},
			}

			err := WriteUserFile(userFilePath, usersToWrite, tc.hashType)
			if err != nil {
				t.Fatalf("WriteUserFile failed: %v", err)
			}

			// --- Read Phase ---
			usersRead, readHashType, err := ReadUserFile(userFilePath)
			if err != nil {
				t.Fatalf("ReadUserFile failed: %v", err)
			}

			if readHashType != tc.hashType {
				t.Errorf("HashType mismatch: got %d, want %d", readHashType, tc.hashType)
			}

			if len(usersRead) != len(usersToWrite) {
				t.Fatalf("Number of users mismatch: got %d, want %d", len(usersRead), len(usersToWrite))
			}

			for username, expectedRecord := range usersToWrite {
				actualRecord, ok := usersRead[username]
				if !ok {
					t.Errorf("User '%s' not found in read data", username)
					continue
				}
				if actualRecord != expectedRecord {
					t.Errorf("UserRecord for '%s' mismatch:\nGot:    %+v\nWanted: %+v", username, actualRecord, expectedRecord)
				}
			}

			// Clean up for next test case
			sys.Remove(userFilePath)
		})
	}
}

func TestReadUserFile_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("non_existent_file", func(t *testing.T) {
		users, hashType, err := ReadUserFile(filepath.Join(tempDir, "nonexistent.db"))
		if err != nil {
			t.Fatalf("ReadUserFile on non-existent file should not return an error, but got: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("Expected 0 users for non-existent file, got %d", len(users))
		}
		if hashType != HashTypeBcrypt {
			t.Errorf("Expected default HashTypeBcrypt for new file, got %d", hashType)
		}
	})

	t.Run("empty_file", func(t *testing.T) {
		emptyFilePath := filepath.Join(tempDir, "empty.db")
		f, _ := os.Create(emptyFilePath)
		f.Close()

		users, hashType, err := ReadUserFile(emptyFilePath)
		if err != nil {
			t.Fatalf("ReadUserFile on empty file should not return an error, but got: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("Expected 0 users for empty file, got %d", len(users))
		}
		if hashType != HashTypeBcrypt {
			t.Errorf("Expected default HashTypeBcrypt for empty file, got %d", hashType)
		}
	})

	t.Run("corrupted_magic_number", func(t *testing.T) {
		corruptedFilePath := filepath.Join(tempDir, "corrupted_magic.db")
		os.WriteFile(corruptedFilePath, []byte{0xDE, 0xAD, 0xBE, 0xEF}, 0644)

		_, _, err := ReadUserFile(corruptedFilePath)
		if err == nil {
			t.Fatal("Expected error for corrupted magic number, but got nil")
		}
	})

	t.Run("unsupported_version", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "unsupported_version.db")
		header := UserFileHeader{
			Magic:   UserFileMagic,
			Version: 99, // Unsupported version
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, &header)
		os.WriteFile(filePath, buf.Bytes(), 0644)

		_, _, err := ReadUserFile(filePath)
		if err == nil {
			t.Fatal("Expected error for unsupported version, but got nil")
		}
	})

	t.Run("unsupported_hash_type", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "unsupported_hash.db")
		header := UserFileHeader{
			Magic:    UserFileMagic,
			Version:  CurrentUserFileVersion,
			HashType: 99, // Unsupported hash type
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, &header)
		os.WriteFile(filePath, buf.Bytes(), 0644)

		_, _, err := ReadUserFile(filePath)
		if err == nil {
			t.Fatal("Expected error for unsupported hash type, but got nil")
		}
	})

	t.Run("truncated_header", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "truncated_header.db")
		// Write only the magic number, which is less than the full header size
		os.WriteFile(filePath, []byte{0x55, 0x53, 0x52, 0x44}, 0644)

		_, _, err := ReadUserFile(filePath)
		if err == nil {
			t.Fatal("Expected error for truncated header, but got nil")
		}
	})

	t.Run("truncated_record", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "truncated_record.db")
		header := UserFileHeader{
			Magic:     UserFileMagic,
			Version:   CurrentUserFileVersion,
			HashType:  HashTypeBcrypt,
			UserCount: 1, // Expects one record
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, &header)
		// Write a partial record (e.g., just the length of the username string)
		binary.Write(&buf, binary.LittleEndian, uint16(10)) // Say username is 10 bytes
		os.WriteFile(filePath, buf.Bytes(), 0644)

		_, _, err := ReadUserFile(filePath)
		if err == nil {
			t.Fatal("Expected error for truncated record, but got nil")
		}
	})

}

func TestHashPassword(t *testing.T) {
	password := "my-secret-password"

	t.Run("bcrypt", func(t *testing.T) {
		hash, err := HashPassword(password, HashTypeBcrypt)
		if err != nil {
			t.Fatalf("HashPassword(bcrypt) failed: %v", err)
		}
		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
			t.Errorf("bcrypt hash verification failed: %v", err)
		}
	})

	t.Run("sha256", func(t *testing.T) {
		hash, err := HashPassword(password, HashTypeSHA256)
		if err != nil {
			t.Fatalf("HashPassword(sha256) failed: %v", err)
		}
		expectedHash := "a9c90c47c231afb31950169ccb89951337eb0689d31660e32c34835bb7018c0c"
		if hash != expectedHash {
			t.Errorf("SHA256 hash mismatch. Got %s, want %s", hash, expectedHash)
		}
	})

	t.Run("sha512", func(t *testing.T) {
		hash, err := HashPassword(password, HashTypeSHA512)
		if err != nil {
			t.Fatalf("HashPassword(sha512) failed: %v", err)
		}
		expectedHash := "c64425af28885bcdc21e925fb6217adbdd50ccc1fadf4c663917f95e7890d19dca40a04e1baefecfbb7a5511492bebd2445c495dff2b8a1b5a910b5a9d82bbda"
		if hash != expectedHash {
			t.Errorf("SHA512 hash mismatch. Got %s, want %s", hash, expectedHash)
		}
	})
}
