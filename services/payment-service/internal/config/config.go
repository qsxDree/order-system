package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port        int
	DatabaseURL string
	RabbitMQURL string
	LogLevel    string
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func Load() Config {
	portStr := getEnv("PORT", getEnv("PAYMENT_PORT", "4003"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 4003
	}

	dbURL := getEnv("PAYMENT_DATABASE_URL", getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/payment_db?sslmode=disable"))
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	logLevel := getEnv("LOG_LEVEL", "info")

	return Config{
		Port:        port,
		DatabaseURL: dbURL,
		RabbitMQURL: rabbitURL,
		LogLevel:    logLevel,
	}
}
