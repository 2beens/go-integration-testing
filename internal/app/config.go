package app

import (
	"errors"
	"os"
)

// Config contains the runtime dependencies and addresses needed to start the app.
type Config struct {
	HTTPAddr     string
	PostgresDSN  string
	RedisAddr    string
	KafkaBrokers []string
	Form3BaseURL string
}

func (c *Config) Validate() error {
	if c.HTTPAddr == "" {
		return errors.New("HTTP_ADDR is required")
	}
	if c.PostgresDSN == "" {
		return errors.New("POSTGRES_DSN is required")
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKER is required")
	}
	if c.Form3BaseURL == "" {
		return errors.New("FORM3_BASE_URL is required")
	}
	return nil
}

// loadConfigFromEnv builds the runtime config from the process environment.
func loadConfigFromEnv() Config {
	return Config{
		HTTPAddr:     os.Getenv("HTTP_ADDR"),
		PostgresDSN:  os.Getenv("POSTGRES_DSN"),
		RedisAddr:    os.Getenv("REDIS_ADDR"),
		KafkaBrokers: []string{os.Getenv("KAFKA_BROKER")},
		Form3BaseURL: os.Getenv("FORM3_BASE_URL"),
	}
}
