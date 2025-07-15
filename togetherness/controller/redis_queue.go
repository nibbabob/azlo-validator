// togetherness/controller/redis_queue.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

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
		Addr:         addr,
		Password:     "", // no password set
		DB:           0,  // use default DB
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})

	// Test the connection
	ctx := context.Background()
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Printf("Connected to Redis: %s", pong)

	return &RedisQueue{
		client:     rdb,
		jobsKey:    "azlo:validator:jobs",
		resultsKey: "azlo:validator:results",
		ctx:        ctx,
	}
}

// PublishJob publishes a validation job to the Redis queue.
func (q *RedisQueue) PublishJob(job shared.ValidationJob) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		log.Printf("Error marshalling job: %v", err)
		return err
	}

	// Use LPUSH to add job to the left of the list
	err = q.client.LPush(q.ctx, q.jobsKey, jobJSON).Err()
	if err != nil {
		log.Printf("Error publishing job to Redis: %v", err)
		return err
	}

	log.Printf("Published job %s for email %s to Redis", job.JobID, job.Email)
	return nil
}

// PublishResult publishes a validation result to the Redis queue.
func (q *RedisQueue) PublishResult(result shared.Result) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("Error marshalling result: %v", err)
		return err
	}

	// Use LPUSH to add result to the left of the list
	err = q.client.LPush(q.ctx, q.resultsKey, resultJSON).Err()
	if err != nil {
		log.Printf("Error publishing result to Redis: %v", err)
		return err
	}

	log.Printf("Published result for job %s, email %s: %s",
		result.JobID, result.Email, result.Status)
	return nil
}

// ConsumeJobs returns a channel for consuming validation jobs.
// Note: This is typically used by workers, not the controller, but required by the interface.
func (q *RedisQueue) ConsumeJobs() (<-chan shared.ValidationJob, error) {
	jobsChan := make(chan shared.ValidationJob, 100)

	go func() {
		defer close(jobsChan)

		for {
			// Blocking pop from the jobs list with 1 second timeout
			res, err := q.client.BRPop(q.ctx, 1*time.Second, q.jobsKey).Result()
			if err != nil {
				// Check if it's a timeout (which is normal)
				if err == redis.Nil {
					continue
				}

				// Check if context was cancelled
				if q.ctx.Err() != nil {
					log.Printf("Context cancelled, stopping job consumption")
					return
				}

				log.Printf("Error consuming job from Redis: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// res is a []string, with the first element being the key, the second the value
			if len(res) >= 2 {
				var job shared.ValidationJob
				if err := json.Unmarshal([]byte(res[1]), &job); err == nil {
					select {
					case jobsChan <- job:
						log.Printf("Consumed job %s for email %s from Redis",
							job.JobID, job.Email)
					case <-q.ctx.Done():
						return
					}
				} else {
					log.Printf("Error unmarshalling job: %v", err)
				}
			}
		}
	}()

	return jobsChan, nil
}

// ConsumeResults returns a channel for consuming validation results.
func (q *RedisQueue) ConsumeResults() (<-chan shared.Result, error) {
	resultsChan := make(chan shared.Result, 100)

	go func() {
		defer close(resultsChan)

		for {
			// Blocking pop from the results list with 1 second timeout
			res, err := q.client.BRPop(q.ctx, 1*time.Second, q.resultsKey).Result()
			if err != nil {
				// Check if it's a timeout (which is normal)
				if err == redis.Nil {
					continue
				}

				// Check if context was cancelled
				if q.ctx.Err() != nil {
					log.Printf("Context cancelled, stopping result consumption")
					return
				}

				log.Printf("Error consuming result from Redis: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// res is a []string, with the first element being the key, the second the value
			if len(res) >= 2 {
				var result shared.Result
				if err := json.Unmarshal([]byte(res[1]), &result); err == nil {
					select {
					case resultsChan <- result:
						log.Printf("Consumed result for job %s, email %s from Redis",
							result.JobID, result.Email)
					case <-q.ctx.Done():
						return
					}
				} else {
					log.Printf("Error unmarshalling result: %v", err)
				}
			}
		}
	}()

	return resultsChan, nil
}

// GetQueueStats returns statistics about the queue.
func (q *RedisQueue) GetQueueStats() map[string]interface{} {
	jobsLen, _ := q.client.LLen(q.ctx, q.jobsKey).Result()
	resultsLen, _ := q.client.LLen(q.ctx, q.resultsKey).Result()

	return map[string]interface{}{
		"pending_jobs":    jobsLen,
		"pending_results": resultsLen,
		"jobs_key":        q.jobsKey,
		"results_key":     q.resultsKey,
	}
}

// ClearQueues clears all queues (useful for testing/development).
func (q *RedisQueue) ClearQueues() error {
	err := q.client.Del(q.ctx, q.jobsKey, q.resultsKey).Err()
	if err != nil {
		log.Printf("Error clearing queues: %v", err)
		return err
	}

	log.Printf("Cleared Redis queues")
	return nil
}

// Close closes the Redis client connection.
func (q *RedisQueue) Close() error {
	log.Printf("Closing Redis connection")
	return q.client.Close()
}
