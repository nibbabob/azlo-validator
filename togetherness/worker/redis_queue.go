// togetherness/worker/redis_queue.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	shared "github.com/nibbabob/azlo-validator-shared"
)

// RedisWorkerQueue implements WorkerQueue using Redis.
type RedisWorkerQueue struct {
	client     *redis.Client
	jobsKey    string
	resultsKey string
	ctx        context.Context
	closed     bool
}

// NewRedisWorkerQueue creates a new Redis-backed worker queue.
func NewRedisWorkerQueue(redisAddr string) *RedisWorkerQueue {
	log.Printf("Connecting to Redis at: %s", redisAddr)

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
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
		log.Fatalf("Worker failed to connect to Redis: %v", err)
	}
	log.Printf("Worker connected to Redis: %s", pong)

	queue := &RedisWorkerQueue{
		client:     rdb,
		jobsKey:    "azlo:validator:jobs",
		resultsKey: "azlo:validator:results",
		ctx:        ctx,
		closed:     false,
	}

	log.Printf("Redis worker queue created successfully")
	return queue
}

// ConsumeJobs returns a channel for consuming validation jobs.
func (q *RedisWorkerQueue) ConsumeJobs() (<-chan shared.ValidationJob, error) {
	log.Printf("Starting job consumption from Redis queue: %s", q.jobsKey)
	jobsChan := make(chan shared.ValidationJob, 10)

	go func() {
		defer close(jobsChan)
		log.Printf("Job consumption goroutine started")

		for !q.closed {
			// Check queue length for debugging
			queueLen, _ := q.client.LLen(q.ctx, q.jobsKey).Result()
			if queueLen > 0 {
				log.Printf("Found %d jobs in Redis queue", queueLen)
			}

			// Blocking pop from the jobs list with 1 second timeout
			log.Printf("Waiting for job from Redis queue...")
			res, err := q.client.BRPop(q.ctx, 1*time.Second, q.jobsKey).Result()
			if err != nil {
				// Check if it's a timeout (which is normal)
				if err == redis.Nil {
					continue
				}

				// Check if context was cancelled or queue is closed
				if q.ctx.Err() != nil || q.closed {
					log.Printf("Worker queue consumption stopped")
					return
				}

				log.Printf("Worker error consuming job from Redis: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("Received data from Redis: %+v", res)

			// res is a []string, with the first element being the key, the second the value
			if len(res) >= 2 {
				var job shared.ValidationJob
				if err := json.Unmarshal([]byte(res[1]), &job); err == nil {
					log.Printf("Successfully unmarshalled job: %s for email: %s", job.JobID, job.Email)
					select {
					case jobsChan <- job:
						log.Printf("Worker consumed job %s for email %s from Redis",
							job.JobID, job.Email)
					case <-q.ctx.Done():
						return
					}
				} else {
					log.Printf("Worker error unmarshalling job: %v, raw data: %s", err, res[1])
				}
			}
		}
	}()

	return jobsChan, nil
}

// PublishResult publishes a validation result back to the controller.
func (q *RedisWorkerQueue) PublishResult(result shared.Result) error {
	if q.closed {
		return nil
	}

	log.Printf("Publishing result for job %s to Redis", result.JobID)

	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("Worker error marshalling result: %v", err)
		return err
	}

	// Use LPUSH to add result to the left of the list
	err = q.client.LPush(q.ctx, q.resultsKey, resultJSON).Err()
	if err != nil {
		log.Printf("Worker error publishing result to Redis: %v", err)
		return err
	}

	log.Printf("Worker published result for job %s, email %s: %s",
		result.JobID, result.Email, result.Status)
	return nil
}

// Close closes the worker queue.
func (q *RedisWorkerQueue) Close() error {
	if !q.closed {
		q.closed = true
		log.Printf("Worker Redis queue closed")
		return q.client.Close()
	}
	return nil
}
