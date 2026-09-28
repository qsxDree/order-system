package events

// OrderItem represents a line item in an order.
type OrderItem struct {
	ProductID string  `json:"productId"`
	SKU       string  `json:"sku"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unitPrice"`
}

// SimulateOptions provides failure-injection flags for deterministic testing of sagas.
type SimulateOptions struct {
	OutOfStock     bool `json:"outOfStock,omitempty"`
	PaymentFailure bool `json:"paymentFailure,omitempty"`
}

// OrderCreatedPayload is emitted when an order is created.
type OrderCreatedPayload struct {
	OrderID     string           `json:"orderId"`
	CustomerID  string           `json:"customerId"`
	Items       []OrderItem      `json:"items"`
	TotalAmount float64          `json:"totalAmount"`
	Currency    string           `json:"currency"`
	Simulate    *SimulateOptions `json:"simulate,omitempty"`
}

// InventoryReservedPayload is emitted when inventory has been successfully reserved.
type InventoryReservedPayload struct {
	OrderID       string `json:"orderId"`
	ReservationID string `json:"reservationId"`
}

// InventoryRejectedPayload is emitted when inventory reservation fails.
type InventoryRejectedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

// PaymentSucceededPayload is emitted when payment processing succeeds.
type PaymentSucceededPayload struct {
	OrderID   string  `json:"orderId"`
	PaymentID string  `json:"paymentId"`
	Amount    float64 `json:"amount"`
}

// PaymentFailedPayload is emitted when payment processing fails.
type PaymentFailedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

// OrderConfirmedPayload is emitted when all saga legs succeed and order is confirmed.
type OrderConfirmedPayload struct {
	OrderID string `json:"orderId"`
}

// OrderCancelledPayload is emitted when a saga leg fails and order is cancelled.
type OrderCancelledPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

// ShipmentDispatchedPayload is emitted when an order is packed and dispatched.
type ShipmentDispatchedPayload struct {
	OrderID        string `json:"orderId"`
	TrackingNumber string `json:"trackingNumber"`
}

// ShipmentDeliveredPayload is emitted when an order is marked delivered.
type ShipmentDeliveredPayload struct {
	OrderID string `json:"orderId"`
}
