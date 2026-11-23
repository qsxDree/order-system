package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ordersys/pkg/testutil"
	"github.com/ordersys/services/order-service/internal/config"
)

func TestCreateOrderRejectsInvalidInputBeforeDatabaseAccess(t *testing.T) {
	valid := `{"customerId":"c1","items":[{"productId":"p1","sku":"S1","quantity":1,"unitPrice":1}]}`
	for _, body := range []string{
		`null`, `{}`, valid + `{}`, strings.Replace(valid, `"quantity":1`, `"quantity":4294967296`, 1),
		strings.Replace(valid, `"unitPrice":1`, `"unitPrice":1.005`, 1),
		strings.Replace(valid, `"productId":"p1"`, `"productId":""`, 1),
		strings.Replace(valid, `"customerId":"c1"`, `"customerId":" "`, 1),
		strings.Replace(valid, `"customerId":"c1"`, `"customerId":"c1","currency":"x"`, 1),
	} {
		t.Run(body, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handleCreateOrder(nil, slog.Default()).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
			}
		})
	}
	rec := httptest.NewRecorder()
	handleCreateOrder(nil, slog.Default()).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(`{"customerId":"`+strings.Repeat("x", 1<<20)+`"}`)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d", rec.Code)
	}
}

func TestCreateOrderStoresMatchingDecimalAmounts(t *testing.T) {
	pool := testutil.NewDB(t, "order")
	router := NewRouter(config.Config{}, pool, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/orders/", strings.NewReader(
		`{"customerId":"c1","items":[{"productId":"p1","sku":"S1","quantity":3,"unitPrice":0.10}]}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
	var response OrderResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.TotalAmount != "0.30" || response.Items[0].UnitPrice != "0.10" {
		t.Fatalf("inconsistent amounts: %+v", response)
	}
	var eventAmount, storedAmount string
	if err := pool.QueryRow(t.Context(), `SELECT payload->>'totalAmount', (SELECT total_amount::text FROM orders) FROM outbox`).Scan(&eventAmount, &storedAmount); err != nil {
		t.Fatal(err)
	}
	if eventAmount != "0.3" || storedAmount != "0.30" {
		t.Fatalf("outbox=%s database=%s", eventAmount, storedAmount)
	}
	pool.Close()
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/orders/"+response.ID, nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("database outage misreported as %d", rec.Code)
	}
}
