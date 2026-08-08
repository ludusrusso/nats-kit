package natscqrs

import (
	"reflect"
	"time"
)

// headerFiller is implemented by *CommandHeader and *EventHeader — a
// pointer receiver, so it is satisfied only by an addressable value. That is
// exactly what hydrateHeader constructs, and exactly why: it lets
// hydrateHeader fill in a missing ID and PublishedAt through this interface
// without needing to know the message's concrete type.
type headerFiller interface {
	fillHeader()
}

// fillHeader fills in h.ID and h.PublishedAt when they are still empty/zero,
// generating a Command-prefixed ID. A header that already has values is
// left untouched.
func (h *CommandHeader) fillHeader() {
	if h.ID == "" {
		h.ID = newID(commandIDPrefix)
	}
	if h.PublishedAt.IsZero() {
		h.PublishedAt = time.Now().UTC()
	}
}

// fillHeader is EventHeader's counterpart to CommandHeader.fillHeader,
// generating an Event-prefixed ID.
func (h *EventHeader) fillHeader() {
	if h.ID == "" {
		h.ID = newID(eventIDPrefix)
	}
	if h.PublishedAt.IsZero() {
		h.PublishedAt = time.Now().UTC()
	}
}

// hydrateHeader returns a copy of m with its embedded header's ID and
// PublishedAt filled in wherever they are still empty/zero. A header field
// that already has a value is left exactly as it was.
//
// m arrives here through a Message interface value, so the struct it holds
// is not addressable and cannot be mutated in place — Set-ing a field
// through an interface's reflect.Value would panic. Instead we build an
// addressable copy with reflect.New, mutate that copy in place through the
// unexported pointer-receiver fillHeader method (reached via the
// headerFiller interface, asserted on the copy's address), and return the
// copy. m itself, and therefore the caller's original value, is never
// touched.
func hydrateHeader(m Message) Message {
	v := reflect.ValueOf(m)
	cp := reflect.New(v.Type()).Elem()
	cp.Set(v)
	if hf, ok := cp.Addr().Interface().(headerFiller); ok {
		hf.fillHeader()
	}
	return cp.Interface().(Message)
}
