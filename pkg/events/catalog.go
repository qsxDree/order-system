package events

import (
	"encoding/json"
	"fmt"
)

// Supported Event Types
const (
	TypeOrderCreated       = "order.created"
	TypeInventoryReserved  = "inventory.reserved"
	TypeInventoryRejected  = "inventory.rejected"
	TypePaymentSucceeded   = "payment.succeeded"
	TypePaymentFailed      = "payment.failed"
	TypeOrderConfirmed     = "order.confirmed"
	TypeOrderCancelled     = "order.cancelled"
	TypeShipmentDispatched = "shipment.dispatched"
	TypeShipmentDelivered  = "shipment.delivered"
)

// AllEventTypes lists all supported domain event types in the system.
var AllEventTypes = []string{
	TypeOrderCreated,
	TypeInventoryReserved,
	TypeInventoryRejected,
	TypePaymentSucceeded,
	TypePaymentFailed,
	TypeOrderConfirmed,
	TypeOrderCancelled,
	TypeShipmentDispatched,
	TypeShipmentDelivered,
}

// IsValidEventType returns true if eventType is a known registered domain event.
func IsValidEventType(eventType string) bool {
	for _, t := range AllEventTypes {
		if t == eventType {
			return true
		}
	}
	return false
}

// UnmarshalPayload unmarshals raw JSON into the corresponding typed event payload struct.
func UnmarshalPayload(eventType string, raw []byte) (any, error) {
	switch eventType {
	case TypeOrderCreated:
		var p OrderCreatedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if err := ValidateOrder(p); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", eventType, err)
		}
		return p, nil

	case TypeInventoryReserved:
		var p InventoryReservedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" || p.ReservationID == "" {
			return nil, fmt.Errorf("%s requires orderId and reservationId", eventType)
		}
		return p, nil

	case TypeInventoryRejected:
		var p InventoryRejectedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" {
			return nil, fmt.Errorf("%s requires orderId", eventType)
		}
		return p, nil

	case TypePaymentSucceeded:
		var p PaymentSucceededPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" || p.PaymentID == "" {
			return nil, fmt.Errorf("%s requires orderId and paymentId", eventType)
		}
		return p, nil

	case TypePaymentFailed:
		var p PaymentFailedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" {
			return nil, fmt.Errorf("%s requires orderId", eventType)
		}
		return p, nil

	case TypeOrderConfirmed:
		var p OrderConfirmedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" {
			return nil, fmt.Errorf("%s requires orderId", eventType)
		}
		return p, nil

	case TypeOrderCancelled:
		var p OrderCancelledPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" {
			return nil, fmt.Errorf("%s requires orderId", eventType)
		}
		return p, nil

	case TypeShipmentDispatched:
		var p ShipmentDispatchedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" || p.TrackingNumber == "" {
			return nil, fmt.Errorf("%s requires orderId and trackingNumber", eventType)
		}
		return p, nil

	case TypeShipmentDelivered:
		var p ShipmentDeliveredPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s: %w", eventType, err)
		}
		if p.OrderID == "" {
			return nil, fmt.Errorf("%s requires orderId", eventType)
		}
		return p, nil

	default:
		return nil, fmt.Errorf("unhandled event type: %s", eventType)
	}
}
