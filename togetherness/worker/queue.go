package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	shared "github.com/nibbabob/azlo-validator-shared"
)

// WorkerQueue defines the interface for worker queue operations.
type WorkerQueue interface {
	ConsumeJobs() (<-chan shared.ValidationJob, error)
	PublishResult(result shared.Result) error
	Close() error
}

// InMemoryWorkerQueue implements WorkerQueue for in-memory communication.
// In production, this would be replaced with Redis, RabbitMQ, etc.
type InMemoryWorkerQueue struct {
	controllerURL string
	jobsChan      chan shared.ValidationJob
	resultsChan   chan shared.Result
	httpClient    *http.Client
	closed        bool
}

// NewWorkerQueue creates a new worker queue based on the queue URL.
func NewWorkerQueue(queueURL string) WorkerQueue {
	if strings.HasPrefix(queueURL, "memory://") {
		return NewInMemoryWorkerQueue()
	}

	// Future: Redis, RabbitMQ implementations
	// if strings.HasPrefix(queueURL, "redis://") {
	//     return NewRedisWorkerQueue(queueURL)
	// }

	log.Printf("Unsupported queue URL: %s, falling back to in-memory", queueURL)
	return NewInMemoryWorkerQueue()
}

// NewInMemoryWorkerQueue creates a new in-memory worker queue.
func NewInMemoryWorkerQueue() *InMemoryWorkerQueue {
	q := &InMemoryWorkerQueue{
		jobsChan:    make(chan shared.ValidationJob, 100),
		resultsChan: make(chan shared.Result, 100),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		closed: false,
	}

	// In a real implementation, this would connect to the controller's queue
	// For now, we simulate it with channels
	go q.simulateJobsFromController()

	return q
}

// ConsumeJobs returns a channel for consuming validation jobs.
func (q *InMemoryWorkerQueue) ConsumeJobs() (<-chan shared.ValidationJob, error) {
	return q.jobsChan, nil
}

// PublishResult publishes a validation result back to the controller.
func (q *InMemoryWorkerQueue) PublishResult(result shared.Result) error {
	if q.closed {
		return nil
	}

	// In a real implementation, this would send the result to the controller
	// via HTTP API, message queue, etc.
	log.Printf("Worker sending result for job %s, email %s: %s",
		result.JobID, result.Email, result.Status)

	// Simulate sending to controller
	select {
	case q.resultsChan <- result:
		return nil
	default:
		log.Printf("Result queue full, dropping result for job %s", result.JobID)
		return nil
	}
}

// Close closes the worker queue.
func (q *InMemoryWorkerQueue) Close() error {
	if !q.closed {
		q.closed = true
		close(q.jobsChan)
		close(q.resultsChan)
		log.Println("Worker queue closed")
	}
	return nil
}

// simulateJobsFromController simulates receiving jobs from the controller.
// In a real implementation, this would poll the controller's API or
// consume from a message queue.
func (q *InMemoryWorkerQueue) simulateJobsFromController() {
	// This is just for demonstration - in production, workers would
	// connect to the actual controller queue

	// Example jobs for testing
	testEmails := []string{
		"test@example.com",
		"invalid@nonexistentdomain12345.com",
		"user@gmail.com",
		"admin@company.com",
	}

	for i, email := range testEmails {
		if q.closed {
			return
		}

		job := shared.ValidationJob{
			JobID:     fmt.Sprintf("test-job-%d", i+1),
			Email:     email,
			Timestamp: time.Now(),
		}

		select {
		case q.jobsChan <- job:
			log.Printf("Simulated job queued: %s for %s", job.JobID, job.Email)
		default:
			log.Printf("Job queue full, skipping test job")
		}

		time.Sleep(5 * time.Second) // Simulate job arrival interval
	}
}

// Note: In production, you would implement additional queue types:

// RedisWorkerQueue for Redis-based queuing
// type RedisWorkerQueue struct {
//     client *redis.Client
//     jobsKey string
//     resultsKey string
// }

// RabbitMQWorkerQueue for RabbitMQ-based queuing
// type RabbitMQWorkerQueue struct {
//     conn *amqp.Connection
//     channel *amqp.Channel
//     jobsQueue string
//     resultsExchange string
// }
