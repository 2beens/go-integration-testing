package main

import (
	"os"

	"github.com/2beens/simple-go-service/internal/app"
)

func configFromEnv() app.Config {
	return app.Config{
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
