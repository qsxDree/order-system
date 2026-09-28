package bus

import (
	cryptoRand "crypto/rand"
	"math"
	"math/big"
	"time"
)

const (
	defaultBaseDelay = 1000 * time.Millisecond
	defaultMaxDelay  = 30000 * time.Millisecond
	defaultMaxJitter = 250 * time.Millisecond
)

func randomJitter() time.Duration {
	n, err := cryptoRand.Int(cryptoRand.Reader, big.NewInt(int64(defaultMaxJitter)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

// CalculateBackoff calculates exponential backoff with randomized jitter for a given retry attempt.
// Formula: min(30s, base * 2^(attempt-1)) + rand(250ms)
func CalculateBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return defaultBaseDelay + randomJitter()
	}

	expFactor := math.Pow(2, float64(attempt-1))
	delayMs := float64(defaultBaseDelay / time.Millisecond) * expFactor

	if delayMs > float64(defaultMaxDelay/time.Millisecond) {
		delayMs = float64(defaultMaxDelay / time.Millisecond)
	}

	return time.Duration(delayMs)*time.Millisecond + randomJitter()
}
