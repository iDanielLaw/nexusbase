package replication

import (
	"testing"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeLag_UsesLastAppliedAt(t *testing.T) {
	now := time.Now()
	resp := &pb.HealthCheckResponse{
		LastAppliedAt: timestamppb.New(now.Add(-5 * time.Second)),
	}

	lag, lastApplied, neg := computeLag(resp, time.Time{}, now)
	if neg {
		t.Fatalf("unexpected negative skew")
	}
	if lastApplied.IsZero() {
		t.Fatalf("expected lastApplied to be set")
	}
	if lag < 4*time.Second || lag > 6*time.Second {
		t.Fatalf("unexpected lag: %v", lag)
	}
}

func TestComputeLag_NegativeClockSkew(t *testing.T) {
	now := time.Now()
	resp := &pb.HealthCheckResponse{
		LastAppliedAt: timestamppb.New(now.Add(5 * time.Second)), // future
	}

	lag, lastApplied, neg := computeLag(resp, time.Time{}, now)
	if !neg {
		t.Fatalf("expected negative skew marker")
	}
	if lag != 0 {
		t.Fatalf("expected zero lag when clock skew detected, got %v", lag)
	}
	if lastApplied.IsZero() {
		t.Fatalf("expected lastApplied to be set")
	}
}

func TestComputeLag_FallbackToPrevPing(t *testing.T) {
	now := time.Now()
	prev := now.Add(-7 * time.Second)
	resp := &pb.HealthCheckResponse{}

	lag, lastApplied, neg := computeLag(resp, prev, now)
	if neg {
		t.Fatalf("unexpected negative skew")
	}
	if !lastApplied.IsZero() {
		t.Fatalf("expected lastApplied to be empty when fallback used")
	}
	if lag < 6*time.Second || lag > 8*time.Second {
		t.Fatalf("unexpected fallback lag: %v", lag)
	}
}
