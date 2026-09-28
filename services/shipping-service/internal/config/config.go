package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port         int
	DatabaseURL  string
	RabbitMQURL  string
	DeliverAfter time.Duration
	LogLevel     string
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func Load() Config {
	portStr := getEnv("PORT", getEnv("SHIPPING_PORT", "4004"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 4004
	}

	dbURL := getEnv("SHIPPING_DATABASE_URL", getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/shipping_db?sslmode=disable"))
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	deliverMsStr := getEnv("DELIVER_AFTER_MS", "5000")
	deliverMs, err := strconv.Atoi(deliverMsStr)
	if err != nil || deliverMs <= 0 {
		deliverMs = 5000
	}

	logLevel := getEnv("LOG_LEVEL", "info")

	return Config{
		Port:         port,
		DatabaseURL:  dbURL,
		RabbitMQURL:  rabbitURL,
		DeliverAfter: time.Duration(deliverMs) * time.Millisecond,
		LogLevel:     logLevel,
	}
}
