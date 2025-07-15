// togetherness/controller/redis_queue.go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/go-redis/redis/v8"
	shared "github.com/nibbabob/azlo-validator-shared"
)

// RedisQueue implements the Queue interface using Redis.
type RedisQueue struct {
	client     *redis.Client
	jobsKey    string
	resultsKey string
	ctx        context.Context
}

// NewRedisQueue creates a new Redis-backed queue.
func NewRedisQueue(addr string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return &RedisQueue{
		client:     rdb,
		jobsKey:    "azlo:validator:jobs",
		resultsKey: "azlo:validator:results",
		ctx:        context.Background(),
	}
}

// PublishJob publishes a validation job to the Redis queue.
func (q *RedisQueue) PublishJob(job shared.ValidationJob) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return q.client.LPush(q.ctx, q.jobsKey, jobJSON).Err()
}

// ConsumeResults returns a channel for consuming validation results.
func (q *RedisQueue) ConsumeResults() (<-chan shared.Result, error) {
	resultsChan := make(chan shared.Result)

	go func() {
		for {
			// Blocking pop from the results list
			res, err := q.client.BRPop(q.ctx, 0, q.resultsKey).Result()
			if err != nil {
				log.Printf("Error consuming result from Redis: %v", err)
				continue
			}

			// res is a []string, with the first element being the key, the second the value
			if len(res) == 2 {
				var result shared.Result
				if err := json.Unmarshal([]byte(res[1]), &result); err == nil {
					resultsChan <- result
				} else {
					log.Printf("Error unmarshalling result: %v", err)
				}
			}
		}
	}()

	return resultsChan, nil
}

// Close closes the Redis client connection.
func (q *RedisQueue) Close() error {
	return q.client.Close()
}
