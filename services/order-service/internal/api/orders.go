package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/outbox"
	"github.com/ordersys/services/order-service/internal/db"
)

type CreateOrderRequest struct {
	CustomerID string                  `json:"customerId"`
	Items      []events.OrderItem      `json:"items"`
	Address    map[string]any          `json:"address,omitempty"`
	Currency   string                  `json:"currency"`
	Simulate   *events.SimulateOptions `json:"simulate,omitempty"`
}

type OrderItemResponse struct {
	ID        string `json:"id"`
	ProductID string `json:"productId"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	UnitPrice string `json:"unitPrice"`
}

type OrderEventResponse struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Producer  string         `json:"producer"`
	Payload   map[string]any `json:"payload"`
	CreatedAt string         `json:"createdAt"`
}

type OrderResponse struct {
	ID                 string               `json:"id"`
	CustomerID         string               `json:"customerId"`
	Status             string               `json:"status"`
	TotalAmount        string               `json:"totalAmount"`
	Currency           string               `json:"currency"`
	InventoryReserved  bool                 `json:"inventoryReserved"`
	PaymentSucceeded   bool                 `json:"paymentSucceeded"`
	CancellationReason *string              `json:"cancellationReason"`
	CreatedAt          string               `json:"createdAt"`
	UpdatedAt          string               `json:"updatedAt"`
	Items              []OrderItemResponse  `json:"items,omitempty"`
	Events             []OrderEventResponse `json:"events,omitempty"`
}

func numericToString(n pgtype.Numeric) string {
	val, err := n.Value()
	if err != nil || val == nil {
		return "0.00"
	}
	return fmt.Sprintf("%v", val)
}

func mapOrderResponse(o db.Order, items []db.OrderItem, eventsList []db.OrderEvent) OrderResponse {
	var reason *string
	if o.CancellationReason.Valid {
		reason = &o.CancellationReason.String
	}

	res := OrderResponse{
		ID:                 o.ID,
		CustomerID:         o.CustomerID,
		Status:             o.Status,
		TotalAmount:        numericToString(o.TotalAmount),
		Currency:           o.Currency,
		InventoryReserved:  o.InventoryReserved,
		PaymentSucceeded:   o.PaymentSucceeded,
		CancellationReason: reason,
		CreatedAt:          o.CreatedAt.Time.Format("2006-01-02T15:04:05.999Z"),
		UpdatedAt:          o.UpdatedAt.Time.Format("2006-01-02T15:04:05.999Z"),
	}

	if items != nil {
		res.Items = make([]OrderItemResponse, 0, len(items))
		for _, item := range items {
			res.Items = append(res.Items, OrderItemResponse{
				ID:        item.ID,
				ProductID: item.ProductID,
				SKU:       item.Sku,
				Quantity:  int(item.Quantity),
				UnitPrice: numericToString(item.UnitPrice),
			})
		}
	}

	if eventsList != nil {
		res.Events = make([]OrderEventResponse, 0, len(eventsList))
		for _, ev := range eventsList {
			var payloadMap map[string]any
			_ = json.Unmarshal(ev.Payload, &payloadMap)
			res.Events = append(res.Events, OrderEventResponse{
				ID:        ev.ID,
				Type:      ev.Type,
				Producer:  ev.Producer,
				Payload:   payloadMap,
				CreatedAt: ev.CreatedAt.Time.Format("2006-01-02T15:04:05.999Z"),
			})
		}
	}

	return res
}

func handleCreateOrder(pool *pgxpool.Pool, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateOrderRequest
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			var sizeErr *http.MaxBytesError
			if errors.As(err, &sizeErr) {
				http.Error(w, `{"error":"body_too_large"}`, http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
			}
			return
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
			return
		}
		if req.Currency == "" {
			req.Currency = "USD"
		}
		total, err := events.OrderTotal(req.Items)
		if err == nil {
			err = events.ValidateOrder(events.OrderCreatedPayload{
				OrderID: "pending", CustomerID: req.CustomerID, Items: req.Items,
				TotalAmount: total, Currency: req.Currency,
			})
		}
		if err != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}

		orderID := uuid.NewString()

		tx, err := pool.Begin(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to begin transaction", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback(r.Context()) }()

		queries := db.New(tx)

		var totalNum pgtype.Numeric
		_ = totalNum.Scan(fmt.Sprintf("%.2f", total))

		var addressBytes []byte
		if req.Address != nil {
			addressBytes, _ = json.Marshal(req.Address)
		}

		order, err := queries.CreateOrder(r.Context(), db.CreateOrderParams{
			ID:                 orderID,
			CustomerID:         req.CustomerID,
			Status:             "PENDING",
			TotalAmount:        totalNum,
			Currency:           req.Currency,
			Address:            addressBytes,
			InventoryReserved:  false,
			PaymentSucceeded:   false,
			CancellationReason: pgtype.Text{Valid: false},
		})
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to insert order", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		insertedItems := make([]db.OrderItem, 0, len(req.Items))

		for _, item := range req.Items {
			var priceNum pgtype.Numeric
			_ = priceNum.Scan(fmt.Sprintf("%.2f", item.UnitPrice))
			itemID := uuid.NewString()

			dbItem, err := queries.CreateOrderItem(r.Context(), db.CreateOrderItemParams{
				ID:        itemID,
				OrderID:   orderID,
				ProductID: item.ProductID,
				Sku:       item.SKU,
				Quantity:  int32(item.Quantity), //nolint:gosec
				UnitPrice: priceNum,
			})
			if err != nil {
				logger.ErrorContext(r.Context(), "failed to insert order item", "err", err)
				http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
				return
			}
			insertedItems = append(insertedItems, dbItem)
		}

		eventPayload := events.OrderCreatedPayload{
			OrderID:     orderID,
			CustomerID:  req.CustomerID,
			Items:       req.Items,
			TotalAmount: total,
			Currency:    req.Currency,
			Simulate:    req.Simulate,
		}
		eventPayloadBytes, _ := json.Marshal(eventPayload)

		_, err = queries.CreateOrderEvent(r.Context(), db.CreateOrderEventParams{
			ID:       uuid.NewString(),
			OrderID:  orderID,
			Type:     events.TypeOrderCreated,
			Producer: "order-service",
			Payload:  eventPayloadBytes,
		})
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to create initial order event", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		err = outbox.Enqueue(r.Context(), tx, orderID, events.TypeOrderCreated, orderID, "", eventPayload)
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to enqueue outbox", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			logger.ErrorContext(r.Context(), "failed to commit order creation", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		logger.InfoContext(r.Context(), "order created", "orderId", orderID, "totalAmount", total)

		res := mapOrderResponse(order, insertedItems, nil)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(res)
	}
}

func handleListOrders(pool *pgxpool.Pool, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		limitStr := r.URL.Query().Get("limit")

		limit := 50
		if limitStr != "" {
			if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
				limit = parsed
				if limit > 200 {
					limit = 200
				}
			}
		}

		queries := db.New(pool)
		orders, err := queries.ListOrders(r.Context(), db.ListOrdersParams{
			Column1: status,
			Limit:   int32(limit), //nolint:gosec
		})
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to list orders", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		resOrders := make([]OrderResponse, 0, len(orders))
		for _, o := range orders {
			items, err := queries.ListOrderItems(r.Context(), o.ID)
			if err != nil {
				logger.ErrorContext(r.Context(), "failed to list order items", "err", err)
				http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
				return
			}
			resOrders = append(resOrders, mapOrderResponse(o, items, nil))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"orders": resOrders})
	}
}

func handleGetOrder(pool *pgxpool.Pool, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" {
			http.Error(w, `{"error":"missing_id"}`, http.StatusBadRequest)
			return
		}

		queries := db.New(pool)
		order, err := queries.GetOrder(r.Context(), id)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
			return
		}
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to get order", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		items, err := queries.ListOrderItems(r.Context(), order.ID)
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to get order items", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}
		eventsList, err := queries.ListOrderEvents(r.Context(), order.ID)
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to get order events", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		res := mapOrderResponse(order, items, eventsList)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	}
}
