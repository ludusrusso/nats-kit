package cronjob

import (
	"testing"
	"time"
)

func TestValidateSchedule(t *testing.T) {
	valid := []string{
		"@every 1s",
		"@every 5m",
		"@every 1h30m",
		"@hourly",
		"@daily",
		"@weekly",
		"@monthly",
		"@yearly",
		"0 0 * * * *",
		"*/30 * * * * *",
	}
	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			if err := validateSchedule(expr); err != nil {
				t.Errorf("validateSchedule(%q) = %v, want nil", expr, err)
			}
		})
	}

	invalid := map[string]string{
		"empty":                  "",
		"whitespace only":        "   ",
		"misspelled directive":   "@evry 5m",
		"misspelled alias":       "@hourl",
		"directive with no unit": "@every",
		"unparsable duration":    "@every soon",
		"sub-second interval":    "@every 500ms",
		"five-field cron":        "* * * * *",
		"seven-field cron":       "* * * * * * *",
	}
	for name, expr := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := validateSchedule(expr); err == nil {
				t.Errorf("validateSchedule(%q) = nil, want an error", expr)
			}
		})
	}
}

func TestScheduleInterval(t *testing.T) {
	cases := map[string]struct {
		want time.Duration
		ok   bool
	}{
		"@every 5m":   {want: 5 * time.Minute, ok: true},
		"@hourly":     {want: time.Hour, ok: true},
		"@daily":      {want: 24 * time.Hour, ok: true},
		"0 0 * * * *": {ok: false},
		"@every soon": {ok: false},
	}
	for expr, want := range cases {
		t.Run(expr, func(t *testing.T) {
			got, ok := scheduleInterval(expr)
			if ok != want.ok || got != want.want {
				t.Errorf("scheduleInterval(%q) = (%s, %t), want (%s, %t)", expr, got, ok, want.want, want.ok)
			}
		})
	}
}
