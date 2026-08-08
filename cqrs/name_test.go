package cqrs

import "testing"

// namedCommand is a Command with a Type() override, exercising the Namer
// path of MessageName.
type namedCommand struct {
	CommandHeader `json:"header"`
	Value         string `json:"value"`
}

func (namedCommand) Type() string { return "custom.name" }

// plainCommand has no override: MessageName must fall back to the Go type
// name.
type plainCommand struct {
	CommandHeader `json:"header"`
	Value         string `json:"value"`
}

// genericBox has a Go type name that includes brackets once instantiated,
// e.g. "cqrs.genericBox[int]" — this must be rejected, not mangled.
type genericBox[T any] struct {
	CommandHeader `json:"header"`
	Value         T `json:"value"`
}

func TestMessageName(t *testing.T) {
	tests := []struct {
		name    string
		msg     Message
		want    string
		wantErr bool
	}{
		{
			name: "default derivation from Go type",
			msg:  plainCommand{},
			want: "cqrs.plainCommand",
		},
		{
			name: "Type() override wins",
			msg:  namedCommand{},
			want: "custom.name",
		},
		{
			name:    "generic instantiation is rejected, not mangled",
			msg:     genericBox[int]{},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MessageName(tc.msg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("MessageName(%#v) = %q, want error", tc.msg, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("MessageName(%#v) returned unexpected error: %v", tc.msg, err)
			}
			if got != tc.want {
				t.Fatalf("MessageName(%#v) = %q, want %q", tc.msg, got, tc.want)
			}
		})
	}
}

// overriddenName lets a single test type return an arbitrary name so every
// invalid-token case can be exercised without declaring a new Go type per
// case.
type overriddenName struct {
	CommandHeader `json:"header"`
	name          string
}

func (o overriddenName) Type() string { return o.name }

func TestMessageName_InvalidSubjectTokens(t *testing.T) {
	invalid := []string{
		"",
		"has space",
		"has\ttab",
		"star*",
		"gt>",
		"pkg.Foo[main.Bar]",
		"slash/not/allowed",
		"emoji-😀",
	}

	for _, name := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := MessageName(overriddenName{name: name})
			if err == nil {
				t.Fatalf("MessageName with override %q: want error, got nil", name)
			}
		})
	}
}

func TestMessageName_ValidSubjectTokens(t *testing.T) {
	valid := []string{
		"a",
		"pkg.Type",
		"pkg.Type_v2",
		"pkg.Type-v2",
		"ALL_CAPS.123",
	}

	for _, name := range valid {
		t.Run(name, func(t *testing.T) {
			got, err := MessageName(overriddenName{name: name})
			if err != nil {
				t.Fatalf("MessageName with override %q: unexpected error: %v", name, err)
			}
			if got != name {
				t.Fatalf("MessageName with override %q = %q, want %q", name, got, name)
			}
		})
	}
}

func TestSubject(t *testing.T) {
	cmdSubject, err := Subject(plainCommand{})
	if err != nil {
		t.Fatalf("Subject(plainCommand{}) returned unexpected error: %v", err)
	}
	if want := "commands.cqrs.plainCommand"; cmdSubject != want {
		t.Fatalf("Subject(plainCommand{}) = %q, want %q", cmdSubject, want)
	}

	type plainEvent struct {
		EventHeader `json:"header"`
	}
	evtSubject, err := Subject(plainEvent{})
	if err != nil {
		t.Fatalf("Subject(plainEvent{}) returned unexpected error: %v", err)
	}
	if want := "events.cqrs.plainEvent"; evtSubject != want {
		t.Fatalf("Subject(plainEvent{}) = %q, want %q", evtSubject, want)
	}
}
