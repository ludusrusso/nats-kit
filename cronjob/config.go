package cronjob

import "log/slog"

// config holds every setting an Option can change.
type config struct {
	logger *slog.Logger
}

func newConfigFrom(opts []Option) *config {
	cfg := &config{logger: slog.Default()}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// Option configures a Runner.
type Option func(*config)

// WithLogger makes the Runner log through logger instead of slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}
