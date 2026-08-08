package cronjob

import (
	"fmt"
	"strings"
	"time"
)

// aliasIntervals maps each predefined Schedule alias to its nominal interval,
// the single taxonomy shared by validateSchedule and scheduleInterval.
var aliasIntervals = map[string]time.Duration{
	"@hourly":  time.Hour,
	"@daily":   24 * time.Hour,
	"@weekly":  7 * 24 * time.Hour,
	"@monthly": 30 * 24 * time.Hour,
	"@yearly":  365 * 24 * time.Hour,
}

// validateSchedule checks the structure of a Schedule expression, so an
// obvious typo is rejected at registration instead of silently producing a
// job that never fires.
func validateSchedule(expr string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return fmt.Errorf("schedule expression is empty")
	}

	switch {
	case strings.HasPrefix(expr, "@every "):
		d, ok := scheduleInterval(expr)
		if !ok {
			return fmt.Errorf("invalid @every duration in %q", expr)
		}
		// NATS scheduled messages reject sub-second @every intervals.
		if d < time.Second {
			return fmt.Errorf("schedule interval must be at least 1s, got %s", d)
		}
		return nil
	case isAlias(expr):
		return nil
	case strings.HasPrefix(expr, "@"):
		return fmt.Errorf("unknown schedule directive %q", expr)
	default:
		// 6-field cron: seconds minutes hours day-of-month month day-of-week.
		if fields := strings.Fields(expr); len(fields) != 6 {
			return fmt.Errorf("cron expression %q must have 6 fields, got %d", expr, len(fields))
		}
		return nil
	}
}

func isAlias(expr string) bool {
	_, ok := aliasIntervals[expr]
	return ok
}

// scheduleInterval derives the firing interval of a Schedule expression,
// reporting false for a cron expression, which has no single interval.
func scheduleInterval(expr string) (time.Duration, bool) {
	expr = strings.TrimSpace(expr)
	if d, ok := aliasIntervals[expr]; ok {
		return d, true
	}
	if raw, ok := strings.CutPrefix(expr, "@every "); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(raw)); err == nil {
			return d, true
		}
	}
	return 0, false
}

// validateName rejects a namespace or job name that would corrupt the
// dot-separated subject space or the underscore-joined consumer name.
func validateName(kind, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("cronjob: %s is empty", kind)
	}
	if strings.ContainsAny(name, ".*> \t\n\r\v\f") {
		return fmt.Errorf("cronjob: %s %q must not contain subject wildcards, dots or whitespace", kind, name)
	}
	return nil
}
