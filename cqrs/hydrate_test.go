package cqrs

import (
	"strings"
	"testing"
	"time"
)

type hydrateFixtureCommand struct {
	CommandHeader `json:"header"`
	Value         string `json:"value"`
}

func TestHydrateHeader_FillsEmptyHeader(t *testing.T) {
	original := hydrateFixtureCommand{Value: "x"}

	got := hydrateHeader(original)

	hydrated, ok := got.(hydrateFixtureCommand)
	if !ok {
		t.Fatalf("hydrateHeader returned %T, want hydrateFixtureCommand", got)
	}
	if hydrated.ID == "" {
		t.Fatal("hydrateHeader left ID empty")
	}
	if !strings.HasPrefix(hydrated.ID, commandIDPrefix) {
		t.Fatalf("hydrated ID %q does not have prefix %q", hydrated.ID, commandIDPrefix)
	}
	if hydrated.PublishedAt.IsZero() {
		t.Fatal("hydrateHeader left PublishedAt zero")
	}
	if hydrated.Value != "x" {
		t.Fatalf("hydrateHeader changed a domain field: Value = %q, want %q", hydrated.Value, "x")
	}
}

func TestHydrateHeader_LeavesPrefilledHeaderAlone(t *testing.T) {
	fixedTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	original := hydrateFixtureCommand{
		CommandHeader: CommandHeader{ID: "cmd_existing", PublishedAt: fixedTime},
		Value:         "x",
	}

	got := hydrateHeader(original)

	hydrated, ok := got.(hydrateFixtureCommand)
	if !ok {
		t.Fatalf("hydrateHeader returned %T, want hydrateFixtureCommand", got)
	}
	if hydrated.ID != "cmd_existing" {
		t.Fatalf("hydrateHeader overwrote a pre-filled ID: got %q, want %q", hydrated.ID, "cmd_existing")
	}
	if !hydrated.PublishedAt.Equal(fixedTime) {
		t.Fatalf("hydrateHeader overwrote a pre-filled PublishedAt: got %v, want %v", hydrated.PublishedAt, fixedTime)
	}
}

func TestHydrateHeader_DoesNotMutateOriginal(t *testing.T) {
	original := hydrateFixtureCommand{Value: "x"}

	_ = hydrateHeader(original)

	if original.ID != "" {
		t.Fatalf("hydrateHeader mutated the caller's original value: ID = %q, want empty", original.ID)
	}
	if !original.PublishedAt.IsZero() {
		t.Fatalf("hydrateHeader mutated the caller's original value: PublishedAt = %v, want zero", original.PublishedAt)
	}
}
