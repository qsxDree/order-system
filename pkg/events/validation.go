package events

import (
	"fmt"
	"math"
	"strings"
)

const maxAmountCents int64 = 999_999_999_999 // NUMERIC(12, 2)

// OrderTotal validates line items and sums integer cents to match stored prices.
func OrderTotal(items []OrderItem) (float64, error) {
	if len(items) == 0 || len(items) > 100 {
		return 0, fmt.Errorf("orders require between 1 and 100 items")
	}
	seen := make(map[string]bool, len(items))
	var total int64
	for _, item := range items {
		if strings.TrimSpace(item.ProductID) == "" || strings.TrimSpace(item.SKU) == "" {
			return 0, fmt.Errorf("productId and sku are required")
		}
		if seen[item.ProductID] {
			return 0, fmt.Errorf("duplicate productId %q", item.ProductID)
		}
		seen[item.ProductID] = true
		if item.Quantity <= 0 || item.Quantity > 1_000_000 {
			return 0, fmt.Errorf("quantity must be between 1 and 1000000")
		}
		cents := math.Round(item.UnitPrice * 100)
		if math.IsNaN(cents) || math.IsInf(cents, 0) || item.UnitPrice < 0 ||
			cents > float64(maxAmountCents) || item.UnitPrice != cents/100 {
			return 0, fmt.Errorf("unitPrice must be a non-negative amount with at most two decimal places")
		}
		lineTotal := int64(cents) * int64(item.Quantity)
		if lineTotal > maxAmountCents-total {
			return 0, fmt.Errorf("order total exceeds supported amount")
		}
		total += lineTotal
	}
	return float64(total) / 100, nil
}

func ValidateOrder(p OrderCreatedPayload) error {
	if strings.TrimSpace(p.OrderID) == "" || strings.TrimSpace(p.CustomerID) == "" {
		return fmt.Errorf("orderId and customerId are required")
	}
	if len(p.Currency) != 3 || strings.IndexFunc(p.Currency, func(r rune) bool { return r < 'A' || r > 'Z' }) >= 0 {
		return fmt.Errorf("currency must be a three-letter uppercase code")
	}
	total, err := OrderTotal(p.Items)
	if err != nil {
		return err
	}
	if math.IsNaN(p.TotalAmount) || math.IsInf(p.TotalAmount, 0) || p.TotalAmount != total {
		return fmt.Errorf("totalAmount must match the sum of the items")
	}
	return nil
}
