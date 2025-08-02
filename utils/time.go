package utils

import "time"

// Clock interface defines methods for getting time.
type Clock interface {
	Now() time.Time
	SetTime(t time.Time)
	// UnixNano() int64 // ถ้าต้องการแค่ Unix Nano
}

var _ Clock = (*SystemClock)(nil)
var _ Clock = (*MockClock)(nil)

// SystemClock is a concrete implementation of Clock that uses the system's current time.
type SystemClock struct{}

// Now returns the current local time.
func (sc *SystemClock) Now() time.Time {
	return time.Now()
}
func (mc *SystemClock) SetTime(t time.Time) {}

// UnixNano returns the current local time as a Unix nanosecond timestamp.
// func (sc *SystemClock) UnixNano() int64 {
// 	return time.Now().UnixNano()
// }

// --- สำหรับการทดสอบ (ไม่จำเป็นต้องรวมใน Production Code ปกติ) ---

// MockClock is a Clock implementation for testing purposes.
type MockClock struct {
	mockTime time.Time
}

// NewMockClock creates a new MockClock with a predefined time.
func NewMockClock(t time.Time) *MockClock {
	return &MockClock{mockTime: t}
}

// Now returns the mock time.
func (mc *MockClock) Now() time.Time {
	return mc.mockTime
}

// SetTime allows updating the mock time during tests.
func (mc *MockClock) SetTime(t time.Time) {
	mc.mockTime = t
}

// UnixNano returns the mock time as a Unix nanosecond timestamp.
// func (mc *MockClock) UnixNano() int60 {
// 	return mc.mockTime.UnixNano()
// }
