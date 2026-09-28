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
	portStr := getEnv("PORT", getEnv("NOTIFICATION_PORT", "4005"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 4005
	}

	dbURL := getEnv("NOTIFICATION_DATABASE_URL", getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/notification_db?sslmode=disable"))
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	logLevel := getEnv("LOG_LEVEL", "info")

	return Config{
		Port:        port,
		DatabaseURL: dbURL,
		RabbitMQURL: rabbitURL,
		LogLevel:    logLevel,
	}
}
