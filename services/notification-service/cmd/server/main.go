package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/database"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/logger"
	"github.com/ordersys/services/notification-service/internal/api"
	"github.com/ordersys/services/notification-service/internal/config"
	"github.com/ordersys/services/notification-service/internal/consumer"
)

func main() {
	cfg := config.Load()
	log := logger.NewLogger("notification-service", cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Initialize Database Pool
	dbPool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect to notification database", "err", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	// 2. Connect to Message Bus
	msgBus := bus.NewMessageBus(bus.Config{
		URL:      cfg.RabbitMQURL,
		Producer: "notification-service",
		Logger:   log,
	})
	if err := msgBus.Connect(ctx, 10, 2*time.Second); err != nil {
		log.Error("failed to connect to rabbitmq", "err", err)
		os.Exit(1)
	}
	defer func() { _ = msgBus.Close() }()

	// 3. Subscribe Handlers
	notifConsumer := consumer.NewNotificationConsumer(dbPool, log)
	err = msgBus.Subscribe(ctx, bus.SubscribeOptions{
		Queue: "notification-service.events",
		RoutingKeys: []string{
			events.TypeOrderConfirmed,
			events.TypeOrderCancelled,
			events.TypeShipmentDispatched,
			events.TypeShipmentDelivered,
		},
		Prefetch:   10,
		MaxRetries: 5,
		Handler:    notifConsumer.HandleMessage,
	})
	if err != nil {
		log.Error("failed to subscribe to events", "err", err)
		os.Exit(1)
	}

	// 4. Start HTTP Server
	router := api.NewRouter()
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Info("notification-service HTTP server listening", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server error", "err", err)
			cancel()
		}
	}()

	// 5. Graceful Shutdown
	select {
	case <-ctx.Done():
	case <-msgBus.Done():
		log.Error("message bus stopped; shutting down for restart")
	}
	cancel()
	log.Info("shutting down notification-service gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP server shutdown error", "err", err)
	}
	log.Info("notification-service stopped")
}
