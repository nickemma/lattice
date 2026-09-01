// Package redis provides the optional shared query-result cache used by the
// remote query service. Local mode deliberately keeps its dependency-free
// in-process cache so the storage and failure exercises remain easy to run.
package redis

import (
	"context"
	"time"

	client "github.com/redis/go-redis/v9"
)

type Cache struct {
	client *client.Client
	ttl    time.Duration
}

func New(rawURL string, ttl time.Duration) (*Cache, error) {
	options, err := client.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Cache{client: client.NewClient(options), ttl: ttl}, nil
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := c.client.Get(ctx, key).Bytes()
	if err == client.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (c *Cache) Set(ctx context.Context, key string, value []byte) error {
	return c.client.Set(ctx, key, value, c.ttl).Err()
}

func (c *Cache) Clear(ctx context.Context) error { return c.client.FlushDB(ctx).Err() }

func (c *Cache) Ready(ctx context.Context) error { return c.client.Ping(ctx).Err() }

func (c *Cache) Close() error { return c.client.Close() }
