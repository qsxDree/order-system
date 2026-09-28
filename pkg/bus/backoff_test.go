package bus_test

import (
	"testing"
	"time"

	"github.com/ordersys/pkg/bus"
)

func TestCalculateBackoff_Bounds(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		delay := bus.CalculateBackoff(attempt)

		// Base delay for attempt 1 is 1000ms + [0, 250ms)
		if attempt == 1 {
			if delay < 1000*time.Millisecond || delay > 1250*time.Millisecond {
				t.Errorf("attempt 1 out of bounds: %v", delay)
			}
		}

		// Cap delay should not exceed 30s + 250ms
		if delay > 30250*time.Millisecond {
			t.Errorf("attempt %d exceeded max cap: %v", attempt, delay)
		}

		// Delay must be positive
		if delay <= 0 {
			t.Errorf("attempt %d produced non-positive delay: %v", attempt, delay)
		}
	}
}
