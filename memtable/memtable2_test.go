package memtable

import (
	"fmt"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/require"
)

// Helpers adapted from memtable_test.go to keep this file standalone.
func makeTestEventValue2(tb testing.TB, val string) []byte {
	tb.Helper()
	if val == "" {
		return nil
	}
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": val})
	require.NoError(tb, err)
	encoded, err := fields.Encode()
	require.NoError(tb, err)
	return encoded
}

func getTestEventValue2(tb testing.TB, data []byte) string {
	tb.Helper()
	if data == nil {
		return ""
	}
	fields, err := core.DecodeFieldsFromBytes(data)
	require.NoError(tb, err)
	if v, ok := fields["value"]; ok {
		if s, ok2 := v.ValueString(); ok2 {
			return s
		}
	}
	return ""
}

// MockSSTableWriter used to validate FlushToSSTable behavior.
type MockSSTableWriter2 struct {
	failAdd bool
	addErr  error
	entries []struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		pointID   uint64
	}
}

func (m *MockSSTableWriter2) Add(key, value []byte, entryType core.EntryType, pointID uint64) error {
	if m.failAdd {
		return m.addErr
	}
	m.entries = append(m.entries, struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		pointID   uint64
	}{key, value, entryType, pointID})
	return nil
}
func (m *MockSSTableWriter2) Finish() error      { return nil }
func (m *MockSSTableWriter2) Abort() error       { return nil }
func (m *MockSSTableWriter2) FilePath() string   { return "mock_path" }
func (m *MockSSTableWriter2) CurrentSize() int64 { return 0 }

func TestMemtable2_PutGet_Basic(t *testing.T) {
	m := NewMemtable2(1024, clock.SystemClockDefault)
	metric := "m1"
	tags := map[string]string{"host": "a"}
	ts := int64(123)

	fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": "42"})
	require.NoError(t, err)

	dp := &core.DataPoint{Metric: metric, Tags: tags, Timestamp: ts, Fields: fv}
	require.NoError(t, m.Put(dp))

	key := core.EncodeTSDBKeyWithString(metric, tags, ts)
	v, et, ok := m.Get(key)
	require.True(t, ok, "expected key to be found")
	require.Equal(t, core.EntryTypePutEvent, et)
	got := getTestEventValue2(t, v)
	require.Equal(t, "42", got)
}

func TestMemtable2_Iterator_Order(t *testing.T) {
	m := NewMemtable2(1<<20, clock.SystemClockDefault)
	metric := "miter"
	tags := map[string]string{"host": "a"}

	// Insert out-of-order timestamps
	dps := []struct {
		ts  int64
		val string
	}{
		{300, "v3"},
		{100, "v1"},
		{200, "v2"},
		{400, "v4"},
	}
	for _, d := range dps {
		fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": d.val})
		require.NoError(t, err)
		dp := &core.DataPoint{Metric: metric, Tags: tags, Timestamp: d.ts, Fields: fv}
		require.NoError(t, m.Put(dp))
	}

	iter := m.NewIterator(nil, nil, types.Ascending)
	defer iter.Close()

	var res []string
	for iter.Next() {
		cur, err := iter.At()
		require.NoError(t, err)
		// decode value
		val := getTestEventValue2(t, cur.Value)
		res = append(res, val)
	}
	require.NoError(t, iter.Error())
	// Expect four results in ascending timestamp order: v1,v2,v3,v4
	require.Equal(t, 4, len(res))
	require.Equal(t, "v1", res[0])
	require.Equal(t, "v2", res[1])
	require.Equal(t, "v3", res[2])
	require.Equal(t, "v4", res[3])
}

func TestMemtable2_FlushToSSTable_WriterError(t *testing.T) {
	m := NewMemtable2(1024, clock.SystemClockDefault)
	require.NoError(t, m.Put(&core.DataPoint{Metric: "x", Tags: map[string]string{"k": "v"}, Timestamp: 1, Fields: func() core.FieldValues {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": "a"})
		return fv
	}()}))

	mockErr := fmt.Errorf("simulated writer add error")
	mockWriter := &MockSSTableWriter2{failAdd: true, addErr: mockErr}

	err := m.FlushToSSTable(mockWriter)
	require.Error(t, err)
	// ensure the error contains the simulated message
	require.Contains(t, err.Error(), mockErr.Error())
}
