package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates and validates a connection to Redis using the provided connection URL.
func NewRedisClient(rawURL string) (*redis.Client, error) {
	if rawURL == "" {
		rawURL = "redis://localhost:6379"
	}

	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url %q: %w", rawURL, err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return client, nil
}

// TryAcquireLock attempts to set a lock key with TTL using SETNX.
// Returns true if the lock was acquired, false if it was already held.
func TryAcquireLock(ctx context.Context, client *redis.Client, key string, value string, ttl time.Duration) (bool, error) {
	return client.SetNX(ctx, key, value, ttl).Result()
}

// ReleaseLock removes the lock key if its value matches.
func ReleaseLock(ctx context.Context, client *redis.Client, key string) error {
	return client.Del(ctx, key).Err()
}
