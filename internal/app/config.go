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
		HTTPAddr:     getEnv("HTTP_ADDR", ":8080"),
		PostgresDSN:  getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/simple_go_service?sslmode=disable"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		Form3BaseURL: getEnv("FORM3_BASE_URL", "http://localhost:9090"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
