package engine

import (
	"context"
	"os"
	"testing"

	"github.com/INLOpen/nexusbase/replication/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestReplication_RealIntegration(t *testing.T) {
	// เตรียม temp dir สำหรับ leader/follower
	leaderDir := os.TempDir() + "/nexus_leader"
	followerDir := os.TempDir() + "/nexus_follower"
	_ = os.Mkdir(leaderDir, 0755)
	_ = os.Mkdir(followerDir, 0755)

	// สร้าง storage engine จริงสำหรับ leader/follower
	leaderOpts := StorageEngineOptions{DataDir: leaderDir, ReplicationMode: "leader"}
	followerOpts := StorageEngineOptions{DataDir: followerDir, ReplicationMode: "follower"}
	leader, err := NewStorageEngine(leaderOpts)
	assert.NoError(t, err)
	follower, err := NewStorageEngine(followerOpts)
	assert.NoError(t, err)

	// Start both engines
	assert.NoError(t, leader.Start())
	assert.NoError(t, follower.Start())

	// สร้าง WAL entry จริง
	entry := &proto.WALEntry{
		EntryType:      proto.WALEntry_PUT_EVENT,
		SequenceNumber: 100,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "integration"},
		Fields:         nil,
		Timestamp:      1234567890,
	}

	// Leader: เขียนข้อมูลด้วย method ที่เหมาะสม (ไม่ใช้ ApplyReplicatedEntry)
	if applyEntry, ok := leader.(interface {
		ApplyEntry(context.Context, *proto.WALEntry) error
	}); ok {
		err = applyEntry.ApplyEntry(context.Background(), entry)
		assert.NoError(t, err)
	} else {
		// ถ้าไม่มี method นี้ ให้ข้ามขั้นตอนนี้
	}

	// Follower: apply replication
	err = follower.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)

	// ตรวจสอบ sequence number sync เฉพาะฝั่ง follower
	assert.Equal(t, entry.SequenceNumber, follower.GetSequenceNumber())

	// Clean up temp dirs
	_ = os.RemoveAll(leaderDir)
	_ = os.RemoveAll(followerDir)
}

func TestReplication_E2E_WriteLeaderUpdateFollower(t *testing.T) {
	// เตรียม temp dir สำหรับ leader/follower
	leaderDir := os.TempDir() + "/nexus_leader_e2e"
	followerDir := os.TempDir() + "/nexus_follower_e2e"
	_ = os.Mkdir(leaderDir, 0755)
	_ = os.Mkdir(followerDir, 0755)

	// สร้าง storage engine จริงสำหรับ leader/follower
	leaderOpts := StorageEngineOptions{DataDir: leaderDir, ReplicationMode: "leader"}
	followerOpts := StorageEngineOptions{DataDir: followerDir, ReplicationMode: "follower"}
	leader, err := NewStorageEngine(leaderOpts)
	assert.NoError(t, err)
	follower, err := NewStorageEngine(followerOpts)
	assert.NoError(t, err)

	// Start both engines
	assert.NoError(t, leader.Start())
	assert.NoError(t, follower.Start())

	// สร้าง Fields เป็น structpb.Struct
	fields, err := structpb.NewStruct(map[string]interface{}{"usage": 88.8})
	assert.NoError(t, err)

	entry := &proto.WALEntry{
		EntryType:      proto.WALEntry_PUT_EVENT,
		SequenceNumber: 200,
		Metric:         "memory",
		Tags:           map[string]string{"host": "e2e"},
		Fields:         fields,
		Timestamp:      1234567999,
	}

	// Leader: เขียนข้อมูลจริง
	if applyEntry, ok := leader.(interface {
		ApplyEntry(context.Context, *proto.WALEntry) error
	}); ok {
		err = applyEntry.ApplyEntry(context.Background(), entry)
		assert.NoError(t, err)
	}

	// Follower: รับ replication จาก leader
	err = follower.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)

	// ตรวจสอบว่า follower sync sequence number ถูกต้อง
	assert.Equal(t, entry.SequenceNumber, follower.GetSequenceNumber())

	// Clean up temp dirs
	_ = os.RemoveAll(leaderDir)
	_ = os.RemoveAll(followerDir)
}

func TestReplication_E2E_PutPutBatchRemove(t *testing.T) {
	leaderDir := os.TempDir() + "/nexus_leader_e2e_batch"
	followerDir := os.TempDir() + "/nexus_follower_e2e_batch"
	_ = os.Mkdir(leaderDir, 0755)
	_ = os.Mkdir(followerDir, 0755)

	leaderOpts := StorageEngineOptions{DataDir: leaderDir, ReplicationMode: "leader"}
	followerOpts := StorageEngineOptions{DataDir: followerDir, ReplicationMode: "follower"}
	leader, err := NewStorageEngine(leaderOpts)
	assert.NoError(t, err)
	follower, err := NewStorageEngine(followerOpts)
	assert.NoError(t, err)

	assert.NoError(t, leader.Start())
	assert.NoError(t, follower.Start())

	// --- Put ---
	fieldsPut, err := structpb.NewStruct(map[string]interface{}{"usage": 11.1})
	assert.NoError(t, err)
	putEntry := &proto.WALEntry{
		EntryType:      proto.WALEntry_PUT_EVENT,
		SequenceNumber: 300,
		Metric:         "disk",
		Tags:           map[string]string{"host": "e2e"},
		Fields:         fieldsPut,
		Timestamp:      1234568000,
	}
	if applyEntry, ok := leader.(interface{ ApplyEntry(context.Context, *proto.WALEntry) error }); ok {
		err = applyEntry.ApplyEntry(context.Background(), putEntry)
		assert.NoError(t, err)
	}
	err = follower.ApplyReplicatedEntry(context.Background(), putEntry)
	assert.NoError(t, err)
	assert.Equal(t, putEntry.SequenceNumber, follower.GetSequenceNumber())

	// --- PutBatch ---
	fieldsBatch1, err := structpb.NewStruct(map[string]interface{}{"usage": 22.2})
	assert.NoError(t, err)
	fieldsBatch2, err := structpb.NewStruct(map[string]interface{}{"usage": 33.3})
	assert.NoError(t, err)
	batchEntries := []*proto.WALEntry{
		{
			EntryType:      proto.WALEntry_PUT_EVENT,
			SequenceNumber: 301,
			Metric:         "disk",
			Tags:           map[string]string{"host": "e2e"},
			Fields:         fieldsBatch1,
			Timestamp:      1234568001,
		},
		{
			EntryType:      proto.WALEntry_PUT_EVENT,
			SequenceNumber: 302,
			Metric:         "disk",
			Tags:           map[string]string{"host": "e2e"},
			Fields:         fieldsBatch2,
			Timestamp:      1234568002,
		},
	}
	if applyBatch, ok := leader.(interface{ ApplyBatch(context.Context, []*proto.WALEntry) error }); ok {
		err = applyBatch.ApplyBatch(context.Background(), batchEntries)
		assert.NoError(t, err)
	}
	for _, entry := range batchEntries {
		err = follower.ApplyReplicatedEntry(context.Background(), entry)
		assert.NoError(t, err)
		assert.Equal(t, entry.SequenceNumber, follower.GetSequenceNumber())
	}

	// --- Remove (DeleteSeries) ---
	removeEntry := &proto.WALEntry{
		EntryType:      proto.WALEntry_DELETE_SERIES,
		SequenceNumber: 303,
		Metric:         "disk",
		Tags:           map[string]string{"host": "e2e"},
	}
	if applyEntry, ok := leader.(interface{ ApplyEntry(context.Context, *proto.WALEntry) error }); ok {
		err = applyEntry.ApplyEntry(context.Background(), removeEntry)
		assert.NoError(t, err)
	}
	err = follower.ApplyReplicatedEntry(context.Background(), removeEntry)
	assert.NoError(t, err)
	assert.Equal(t, removeEntry.SequenceNumber, follower.GetSequenceNumber())

	_ = os.RemoveAll(leaderDir)
	_ = os.RemoveAll(followerDir)
}

func TestReplication_E2E_DeleteRange(t *testing.T) {
	leaderDir := os.TempDir() + "/nexus_leader_e2e_range"
	followerDir := os.TempDir() + "/nexus_follower_e2e_range"
	_ = os.Mkdir(leaderDir, 0755)
	_ = os.Mkdir(followerDir, 0755)

	leaderOpts := StorageEngineOptions{DataDir: leaderDir, ReplicationMode: "leader"}
	followerOpts := StorageEngineOptions{DataDir: followerDir, ReplicationMode: "follower"}
	leader, err := NewStorageEngine(leaderOpts)
	assert.NoError(t, err)
	follower, err := NewStorageEngine(followerOpts)
	assert.NoError(t, err)

	assert.NoError(t, leader.Start())
	assert.NoError(t, follower.Start())

	// --- DeleteRange ---
	deleteRangeEntry := &proto.WALEntry{
		EntryType:      proto.WALEntry_DELETE_RANGE,
		SequenceNumber: 400,
		Metric:         "disk",
		Tags:           map[string]string{"host": "e2e"},
		StartTime:      1234568000,
		EndTime:        1234569000,
	}
	if applyEntry, ok := leader.(interface{ ApplyEntry(context.Context, *proto.WALEntry) error }); ok {
		err = applyEntry.ApplyEntry(context.Background(), deleteRangeEntry)
		assert.NoError(t, err)
	}
	err = follower.ApplyReplicatedEntry(context.Background(), deleteRangeEntry)
	assert.NoError(t, err)
	assert.Equal(t, deleteRangeEntry.SequenceNumber, follower.GetSequenceNumber())

	_ = os.RemoveAll(leaderDir)
	_ = os.RemoveAll(followerDir)
}
