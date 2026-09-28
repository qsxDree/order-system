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
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/logger"
	"github.com/ordersys/services/order-service/internal/api"
	"github.com/ordersys/services/order-service/internal/config"
	"github.com/ordersys/services/order-service/internal/db"
	"github.com/ordersys/services/order-service/internal/outbox"
	"github.com/ordersys/services/order-service/internal/saga"
)

func main() {
	cfg := config.Load()
	log := logger.NewLogger("order-service", cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Database Connection Pool
	dbPool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	// 2. Message Bus
	msgBus := bus.NewMessageBus(bus.Config{
		URL:      cfg.RabbitMQURL,
		Producer: "order-service",
		Logger:   log,
	})
	if err := msgBus.Connect(ctx, 10, 2*time.Second); err != nil {
		log.Error("failed to connect to rabbitmq", "err", err)
		os.Exit(1)
	}
	defer func() { _ = msgBus.Close() }()

	// 3. Outbox Relay
	relay := outbox.NewRelay(dbPool, msgBus, log)
	relay.Start(200 * time.Millisecond)
	defer relay.Stop()

	// 4. Saga Coordinator Consumer
	coord := saga.NewCoordinator(dbPool, log)
	err = msgBus.Subscribe(ctx, bus.SubscribeOptions{
		Queue: "order-service.saga",
		RoutingKeys: []string{
			events.TypeInventoryReserved,
			events.TypePaymentSucceeded,
			events.TypeInventoryRejected,
			events.TypePaymentFailed,
			events.TypeShipmentDispatched,
			events.TypeShipmentDelivered,
		},
		Prefetch:   10,
		MaxRetries: 5,
		Handler:    coord.HandleMessage,
	})
	if err != nil {
		log.Error("failed to subscribe to saga events", "err", err)
		os.Exit(1)
	}

	// 5. HTTP Server
	router := api.NewRouter(cfg, dbPool, msgBus, log)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("order-service HTTP server listening", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server error", "err", err)
		}
	}()

	// 6. Graceful Shutdown
	<-ctx.Done()
	log.Info("shutting down order-service gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP server shutdown error", "err", err)
	}
	log.Info("order-service stopped")
}
