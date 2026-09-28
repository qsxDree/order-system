package config

import (
	"os"
	"strconv"
)

type ServicesConfig struct {
	Inventory    string
	Payment      string
	Shipping     string
	Notification string
}

type MgmtConfig struct {
	URL  string
	User string
	Pass string
}

type Config struct {
	ServiceName string
	Port        int
	DatabaseURL string
	RabbitMQURL string
	Services    ServicesConfig
	Mgmt        MgmtConfig
	LogLevel    string
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func Load() Config {
	portStr := getEnv("PORT", getEnv("ORDER_PORT", "4001"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 4001
	}

	dbURL := getEnv("ORDER_DATABASE_URL", getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/order_db?sslmode=disable"))
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	return Config{
		ServiceName: getEnv("SERVICE_NAME", "order-service"),
		Port:        port,
		DatabaseURL: dbURL,
		RabbitMQURL: rabbitURL,
		Services: ServicesConfig{
			Inventory:    getEnv("INVENTORY_URL", "http://localhost:4002"),
			Payment:      getEnv("PAYMENT_URL", "http://localhost:4003"),
			Shipping:     getEnv("SHIPPING_URL", "http://localhost:4004"),
			Notification: getEnv("NOTIFICATION_URL", "http://localhost:4005"),
		},
		Mgmt: MgmtConfig{
			URL:  getEnv("RABBITMQ_MGMT_URL", "http://localhost:15672"),
			User: getEnv("RABBITMQ_MGMT_USER", "guest"),
			Pass: getEnv("RABBITMQ_MGMT_PASS", "guest"),
		},
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}
}
