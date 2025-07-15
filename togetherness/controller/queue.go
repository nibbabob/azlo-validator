package main

import (
	"log"

	shared "github.com/nibbabob/azlo-validator-shared"
)

// InMemoryQueue implements the Queue interface using Go channels.
// In production, this would be replaced with Redis, RabbitMQ, etc.
type InMemoryQueue struct {
	jobsChan    chan shared.ValidationJob
	resultsChan chan shared.Result
	closed      bool
}

// NewInMemoryQueue creates a new in-memory queue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		jobsChan:    make(chan shared.ValidationJob, 1000),
		resultsChan: make(chan shared.Result, 1000),
		closed:      false,
	}
}

// PublishJob publishes a validation job to the queue.
func (q *InMemoryQueue) PublishJob(job shared.ValidationJob) error {
	if q.closed {
		return nil
	}

	select {
	case q.jobsChan <- job:
		log.Printf("Published job %s for email %s", job.JobID, job.Email)
		return nil
	default:
		log.Printf("Job queue is full, dropping job %s", job.JobID)
		return nil
	}
}

// ConsumeJobs returns a channel for consuming validation jobs.
func (q *InMemoryQueue) ConsumeJobs() (<-chan shared.ValidationJob, error) {
	return q.jobsChan, nil
}

// PublishResult publishes a validation result to the queue.
func (q *InMemoryQueue) PublishResult(result shared.Result) error {
	if q.closed {
		return nil
	}

	select {
	case q.resultsChan <- result:
		log.Printf("Published result for job %s, email %s: %s",
			result.JobID, result.Email, result.Status)
		return nil
	default:
		log.Printf("Result queue is full, dropping result for job %s", result.JobID)
		return nil
	}
}

// ConsumeResults returns a channel for consuming validation results.
func (q *InMemoryQueue) ConsumeResults() (<-chan shared.Result, error) {
	return q.resultsChan, nil
}

// Close closes the queue channels.
func (q *InMemoryQueue) Close() error {
	if !q.closed {
		q.closed = true
		close(q.jobsChan)
		close(q.resultsChan)
		log.Println("Queue closed")
	}
	return nil
}

// GetJobsChannel returns the jobs channel (for worker access).
func (q *InMemoryQueue) GetJobsChannel() <-chan shared.ValidationJob {
	return q.jobsChan
}

// GetResultsChannel returns the results channel (for worker access).
func (q *InMemoryQueue) GetResultsChannel() chan<- shared.Result {
	return q.resultsChan
}
