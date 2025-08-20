package replication

import (
	"context"
	"fmt"
	"log/slog"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
)

// Applier is responsible for taking WAL entries received from a leader
// and applying them to a local storage engine.
type Applier struct {
	engine engine.StorageEngineInterface
	logger *slog.Logger
}

// NewApplier creates a new replication applier.
func NewApplier(eng engine.StorageEngineInterface, logger *slog.Logger) *Applier {
	return &Applier{
		engine: eng,
		logger: logger.With("component", "ReplicationApplier"),
	}
}

// ApplyEntry converts a protobuf WALEntry to a core WALEntry and applies it
// to the storage engine.
func (a *Applier) ApplyEntry(ctx context.Context, apiEntry *apiv1.WALEntry) error {
	coreEntry, err := a.convertAPIToCoreWALEntry(apiEntry)
	if err != nil {
		a.logger.Error("Failed to convert API WAL entry to core entry", "seq_num", apiEntry.GetSequenceNumber(), "error", err)
		return err
	}

	if err := a.engine.ApplyReplicatedEntry(ctx, coreEntry); err != nil {
		a.logger.Error("Failed to apply replicated entry to engine", "seq_num", coreEntry.SeqNum, "error", err)
		return err
	}

	a.logger.Debug("Successfully applied replicated entry", "seq_num", coreEntry.SeqNum, "type", coreEntry.EntryType)
	return nil
}

// convertAPIToCoreWALEntry converts the gRPC API WALEntry message to the internal
// core.WALEntry struct that the engine understands.
func (a *Applier) convertAPIToCoreWALEntry(apiEntry *apiv1.WALEntry) (*core.WALEntry, error) {
	coreEntry := &core.WALEntry{
		SeqNum:       apiEntry.GetSequenceNumber(),
		SegmentIndex: apiEntry.GetWalSegmentIndex(),
	}

	switch payload := apiEntry.Payload.(type) {
	case *apiv1.WALEntry_PutEvent:
		coreEntry.EntryType = core.EntryTypePutEvent
		coreEntry.Key = payload.PutEvent.GetKey()
		coreEntry.Value = payload.PutEvent.GetValue()
		// NEW: Reconstruct the DataPoint on the follower side from the protobuf message.
		// This is crucial for the engine to be able to create the necessary metadata.
		fields, err := core.DecodeFieldsFromBytes(payload.PutEvent.GetValue())
		if err != nil {
			// If we can't decode the fields, we cannot proceed reliably.
			return nil, fmt.Errorf("failed to decode fields from replicated PutEvent: %w", err)
		}
		coreEntry.DataPoint.Metric = payload.PutEvent.GetMetric()
		coreEntry.DataPoint.Tags = payload.PutEvent.GetTags()
		coreEntry.DataPoint.Timestamp = payload.PutEvent.GetTimestamp()
		coreEntry.DataPoint.Fields = fields
	case *apiv1.WALEntry_DeleteEvent:
		coreEntry.EntryType = core.EntryTypeDelete
		coreEntry.Key = payload.DeleteEvent.GetKey()
	case *apiv1.WALEntry_DeleteSeriesEvent:
		coreEntry.EntryType = core.EntryTypeDeleteSeries
		coreEntry.Key = payload.DeleteSeriesEvent.GetKeyPrefix()
	case *apiv1.WALEntry_DeleteRangeEvent:
		coreEntry.EntryType = core.EntryTypeDeleteRange
		coreEntry.Key = payload.DeleteRangeEvent.GetKeyPrefix()
		// Re-encode start/end timestamps into the value field for the engine.
		coreEntry.Value = core.EncodeRangeTombstoneValue(
			payload.DeleteRangeEvent.GetStartTs(),
			payload.DeleteRangeEvent.GetEndTs(),
		)
	default:
		return nil, fmt.Errorf("unknown WAL entry payload type: %T", payload)
	}

	return coreEntry, nil
}