package natscqrs

import (
	"encoding/json"
	"reflect"
	"testing"
)

// goodOrder embeds its header correctly, with the load-bearing json:"header"
// tag.
type goodOrder struct {
	CommandHeader `json:"header"`
	CustomerID    string `json:"customer_id"`
}

// noTagOrder forgets the json:"header" tag: the header's fields are
// promoted inline into the message's own JSON.
type noTagOrder struct {
	CommandHeader
	CustomerID string `json:"customer_id"`
}

// collidingOrder also forgets the tag, and additionally has a domain field
// whose json tag collides with the header's "id" field. This is the silent
// case called out in the spec: the header's id vanishes from the JSON with
// no error from encoding/json.
type collidingOrder struct {
	CommandHeader
	DomainID string `json:"id"`
}

func TestCheckHeaderTag(t *testing.T) {
	tests := []struct {
		name    string
		zero    any
		wantErr bool
	}{
		{name: "correctly tagged header", zero: goodOrder{}, wantErr: false},
		{name: "missing json:\"header\" tag", zero: noTagOrder{}, wantErr: true},
		{name: "domain field collides with id, header inline", zero: collidingOrder{}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkHeaderTag(reflect.TypeOf(tc.zero))
			if tc.wantErr && err == nil {
				t.Fatalf("checkHeaderTag(%T): want error, got nil", tc.zero)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("checkHeaderTag(%T): unexpected error: %v", tc.zero, err)
			}
		})
	}
}

// TestCheckHeaderTag_CollisionActuallyVanishes documents, as an assertion
// rather than a comment, the exact silent failure the validator exists to
// catch: with an inline header and a colliding domain field, the header's
// id is genuinely absent from the marshaled JSON.
func TestCheckHeaderTag_CollisionActuallyVanishes(t *testing.T) {
	data, err := json.Marshal(collidingOrder{
		CommandHeader: CommandHeader{ID: "cmd_should_vanish"},
		DomainID:      "domain-id",
	})
	if err != nil {
		t.Fatalf("json.Marshal: unexpected error: %v", err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("json.Unmarshal: unexpected error: %v", err)
	}
	if _, ok := top["header"]; ok {
		t.Fatalf("expected no top-level \"header\" field, got one in %s", data)
	}
	var id string
	if err := json.Unmarshal(top["id"], &id); err != nil {
		t.Fatalf("json.Unmarshal id: unexpected error: %v", err)
	}
	if id != "domain-id" {
		t.Fatalf("top-level id = %q, want the colliding domain field's value %q (the header's id must have vanished)", id, "domain-id")
	}
}

func TestValidateHeaderTag_Cached(t *testing.T) {
	t1 := reflect.TypeOf(goodOrder{})
	if err := validateHeaderTag(t1); err != nil {
		t.Fatalf("validateHeaderTag: unexpected error: %v", err)
	}
	// Calling it again for the same type must return the same (cached)
	// result.
	if err := validateHeaderTag(t1); err != nil {
		t.Fatalf("validateHeaderTag (second call): unexpected error: %v", err)
	}

	t2 := reflect.TypeOf(noTagOrder{})
	if err := validateHeaderTag(t2); err == nil {
		t.Fatalf("validateHeaderTag(noTagOrder): want error, got nil")
	}
	if err := validateHeaderTag(t2); err == nil {
		t.Fatalf("validateHeaderTag(noTagOrder) (second call): want error, got nil")
	}
}

func TestValidateMessageType(t *testing.T) {
	if err := validateMessageType(goodOrder{}); err != nil {
		t.Fatalf("validateMessageType(goodOrder{}): unexpected error: %v", err)
	}
	if err := validateMessageType(noTagOrder{}); err == nil {
		t.Fatalf("validateMessageType(noTagOrder{}): want error, got nil")
	}
}

// sneakyOrder is the false negative checkHeaderTag alone cannot catch: its
// CommandHeader is untagged, so it is promoted inline (its ID ends up at
// the top level), and it additionally has a domain field, itself named
// Header and tagged json:"header", that contains an "id" of its own. The
// marshaled JSON therefore does have a top-level "header" object with an
// "id" field — checkHeaderTag's shape check is satisfied — even though that
// object is not the real header at all, and the real header's ID sits
// unprotected at the top level instead.
type sneakyOrder struct {
	CommandHeader
	Header struct {
		ID string `json:"id"`
	} `json:"header"`
}

// TestCheckHeaderTag_FalseNegative documents, empirically, that the
// shape-only check is genuinely fooled by sneakyOrder: this is the reachable
// false negative checkHeaderEmbedding exists to close.
func TestCheckHeaderTag_FalseNegative(t *testing.T) {
	data, err := json.Marshal(sneakyOrder{
		CommandHeader: CommandHeader{ID: "cmd_real"},
		Header: struct {
			ID string `json:"id"`
		}{ID: "not-a-real-header"},
	})
	if err != nil {
		t.Fatalf("json.Marshal: unexpected error: %v", err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("json.Unmarshal: unexpected error: %v", err)
	}
	var topID string
	if err := json.Unmarshal(top["id"], &topID); err != nil {
		t.Fatalf("json.Unmarshal top-level id: unexpected error: %v", err)
	}
	if topID != "cmd_real" {
		t.Fatalf("top-level id = %q, want %q (the real header, promoted inline)", topID, "cmd_real")
	}

	var header struct{ ID string }
	if err := json.Unmarshal(top["header"], &header); err != nil {
		t.Fatalf("json.Unmarshal header: unexpected error: %v", err)
	}
	if header.ID != "not-a-real-header" {
		t.Fatalf("header.id = %q, want %q (the colliding domain field)", header.ID, "not-a-real-header")
	}

	// The shape-only check is fooled: it sees a top-level "header" object
	// with an "id" field and reports no error.
	if err := checkHeaderTag(reflect.TypeOf(sneakyOrder{})); err != nil {
		t.Fatalf("checkHeaderTag(sneakyOrder{}): want the shape-only check to be fooled (nil error), got: %v", err)
	}
}

// TestCheckHeaderEmbedding_CatchesFalseNegative confirms the reflection-based
// structural check is not fooled by sneakyOrder: it looks at the embedded
// CommandHeader field itself, sees it has no json:"header" tag, and rejects
// it — regardless of what a colliding domain field's own tag happens to be.
func TestCheckHeaderEmbedding_CatchesFalseNegative(t *testing.T) {
	err := checkHeaderEmbedding(reflect.TypeOf(sneakyOrder{}))
	if err == nil {
		t.Fatal("checkHeaderEmbedding(sneakyOrder{}): want error, got nil")
	}
}

// TestValidateHeaderTag_CatchesFalseNegative confirms the actual entry
// point Marshal and Handler.Validate use — validateHeaderTag, which now
// runs checkHeaderEmbedding before checkHeaderTag — rejects sneakyOrder,
// even though checkHeaderTag alone would not.
func TestValidateHeaderTag_CatchesFalseNegative(t *testing.T) {
	if err := validateHeaderTag(reflect.TypeOf(sneakyOrder{})); err == nil {
		t.Fatal("validateHeaderTag(sneakyOrder{}): want error, got nil")
	}
	if _, err := Marshal(sneakyOrder{}); err == nil {
		t.Fatal("Marshal(sneakyOrder{}): want error, got nil")
	}
}

func TestCheckHeaderEmbedding_RejectsNoHeaderField(t *testing.T) {
	type noHeaderAtAll struct {
		Value string `json:"value"`
	}
	if err := checkHeaderEmbedding(reflect.TypeOf(noHeaderAtAll{})); err == nil {
		t.Fatal("checkHeaderEmbedding(noHeaderAtAll{}): want error, got nil")
	}
}

func TestCheckHeaderEmbedding_AcceptsGoodOrder(t *testing.T) {
	if err := checkHeaderEmbedding(reflect.TypeOf(goodOrder{})); err != nil {
		t.Fatalf("checkHeaderEmbedding(goodOrder{}): unexpected error: %v", err)
	}
}
