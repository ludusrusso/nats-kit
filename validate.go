package natscqrs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

// headerTagCache memoizes checkHeader's result per reflect.Type, so that
// after a Message type's first check, validating it again — on every
// Marshal call, or every Handler.Validate call — costs nothing.
var headerTagCache sync.Map // map[reflect.Type]headerTagResult

// headerTagResult wraps an error so a "no error" result can be stored in
// headerTagCache as a concrete, non-nil value. Storing a bare nil error
// directly would make it indistinguishable, on Load, from "not cached yet",
// and a bare nil interface{} cannot be safely asserted back to error.
type headerTagResult struct{ err error }

// validateHeaderTag confirms that t declares its embedded header correctly,
// caching the result for t.
func validateHeaderTag(t reflect.Type) error {
	if v, ok := headerTagCache.Load(t); ok {
		return v.(headerTagResult).err
	}
	res := headerTagResult{err: checkHeader(t)}
	actual, _ := headerTagCache.LoadOrStore(t, res)
	return actual.(headerTagResult).err
}

// commandHeaderType and eventHeaderType are compared against by
// checkHeaderEmbedding to recognize an embedded header field regardless of
// its json tag.
var (
	commandHeaderType = reflect.TypeOf(CommandHeader{})
	eventHeaderType   = reflect.TypeOf(EventHeader{})
)

// checkHeader is the combined check validateHeaderTag caches: it runs
// checkHeaderEmbedding, the primary, reflection-based check, and then
// checkHeaderTag, a secondary, marshal-based check that catches what
// checkHeaderEmbedding's field-level view cannot see (see checkHeaderTag's
// doc comment).
func checkHeader(t reflect.Type) error {
	if err := checkHeaderEmbedding(t); err != nil {
		return err
	}
	return checkHeaderTag(t)
}

// checkHeaderEmbedding is the primary header check: it walks t's own
// declared fields — structure, not the JSON encoding/json happens to
// produce from it — looking for the anonymous CommandHeader or EventHeader
// field every Message must embed, and requires that field to sit at depth
// 1 (a direct field of t, not embedded indirectly through another embedded
// struct) with a json tag of exactly "header".
//
// This is what checkHeaderTag (below), which only inspects the *shape* of
// a marshaled zero value, cannot do: shape alone cannot distinguish a
// correctly-tagged header from an untagged, inline-promoted header that
// merely happens to sit next to a domain field which is itself tagged
// json:"header" and contains an "id" — e.g.
//
//	type sneakyCommand struct {
//		CommandHeader                       // no tag: promoted inline
//		Header        struct{ ID string `json:"id"` } `json:"header"`
//	}
//
// marshals to {"id":"<real id>","header":{"id":"not-a-real-header"}}: a
// top-level "header" object containing "id" is present, so checkHeaderTag
// alone passes it, even though the real header was promoted to the top
// level (the wrong wire format) and a caller reading the "header" object
// would get the domain field's id instead. checkHeaderEmbedding rejects
// this immediately: CommandHeader is an anonymous field of sneakyCommand at
// depth 1, and its json tag is "" — not "header".
func checkHeaderEmbedding(t reflect.Type) error {
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("natscqrs: %s is not a struct; a Message must embed CommandHeader or EventHeader", t)
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous || (f.Type != commandHeaderType && f.Type != eventHeaderType) {
			continue
		}
		tag, ok := f.Tag.Lookup("json")
		if ok && tag == "header" {
			return nil
		}
		if !ok || tag == "" {
			return fmt.Errorf(
				"natscqrs: %s embeds %s as field %q with no json tag; Go promotes its ID/PublishedAt/Metadata "+
					"fields inline into %s's own JSON — add an explicit `json:\"header\"` tag to field %q",
				t, f.Type, f.Name, t, f.Name,
			)
		}
		return fmt.Errorf(
			"natscqrs: %s embeds %s as field %q with json tag %q, want exactly `json:\"header\"`",
			t, f.Type, f.Name, tag,
		)
	}
	return fmt.Errorf(
		"natscqrs: %s has no CommandHeader or EventHeader field embedded at its top level "+
			"(depth 1); embed one with an explicit `json:\"header\"` tag",
		t,
	)
}

// checkHeaderTag is the secondary header check, run after
// checkHeaderEmbedding has already passed: it confirms that t's JSON form
// has a top-level "header" object containing an "id" field, i.e. that t
// embeds CommandHeader or EventHeader with an explicit json:"header" tag:
//
//	type CreateOrder struct {
//		natscqrs.CommandHeader `json:"header"`
//		CustomerID string `json:"customer_id"`
//	}
//
// checkHeaderEmbedding already rejects a missing or wrongly-tagged header
// field by looking at t's declared fields directly, which is what catches
// the sneaky false negative described in its doc comment and gives the
// better, field-naming error message. But a field-level view cannot see
// everything: if a domain field is declared with the exact same json tag
// as the header field itself — both `json:"header"`, at the same nesting
// depth — encoding/json resolves the tie by dropping both fields, and the
// header vanishes from the JSON entirely even though checkHeaderEmbedding
// saw a correctly-tagged header field. checkHeaderTag catches that
// remaining case (and, redundantly but harmlessly, everything
// checkHeaderEmbedding already catches) by marshaling a zero value of t and
// inspecting the actual JSON shape it produces: any of these mistakes
// results in there being no top-level "header" object, which is what is
// checked for.
func checkHeaderTag(t reflect.Type) error {
	zero := reflect.New(t).Elem().Interface()
	data, err := json.Marshal(zero)
	if err != nil {
		return fmt.Errorf("natscqrs: %s: failed to marshal a zero value while validating its header: %w", t, err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return fmt.Errorf("natscqrs: %s: message must marshal to a JSON object, got: %s", t, data)
	}

	headerRaw, ok := top["header"]
	if !ok {
		return fmt.Errorf(
			"natscqrs: %s has no top-level \"header\" field in its JSON form; "+
				"embed CommandHeader or EventHeader with an explicit `json:\"header\"` tag "+
				"(a missing tag, or a domain field whose json tag collides with "+
				"\"id\", \"published_at\" or \"metadata\", both silently destroy the message header "+
				"with no error from encoding/json)",
			t,
		)
	}

	var header map[string]json.RawMessage
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return fmt.Errorf("natscqrs: %s: \"header\" field is not a JSON object", t)
	}
	if _, ok := header["id"]; !ok {
		return fmt.Errorf(
			"natscqrs: %s: \"header\" object has no \"id\" field; "+
				"embed CommandHeader or EventHeader with an explicit `json:\"header\"` tag",
			t,
		)
	}
	return nil
}

// validateMessageType checks that m's Go type will produce a spec-compliant
// envelope: a properly embedded and tagged header (checkHeader) and a valid
// Message Name (MessageName). Marshal and Handler.Validate both route
// through this, so a misdeclared Message type is rejected identically
// everywhere.
func validateMessageType(m Message) error {
	if err := validateHeaderTag(reflect.TypeOf(m)); err != nil {
		return err
	}
	if _, err := MessageName(m); err != nil {
		return err
	}
	return nil
}
