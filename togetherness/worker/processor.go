package main

import (
	"context"
	"log"
	"sync"
	"time"

	shared "github.com/nibbabob/azlo-validator-shared"
)

// JobProcessor handles the processing of validation jobs.
type JobProcessor struct {
	workerID       string
	validator      *shared.Validator
	queue          WorkerQueue
	maxConcurrency int
	semaphore      chan struct{}
	wg             sync.WaitGroup
}

// NewJobProcessor creates a new job processor.
func NewJobProcessor(workerID string, validator *shared.Validator, queue WorkerQueue, maxConcurrency int) *JobProcessor {
	return &JobProcessor{
		workerID:       workerID,
		validator:      validator,
		queue:          queue,
		maxConcurrency: maxConcurrency,
		semaphore:      make(chan struct{}, maxConcurrency),
	}
}

// Start begins processing jobs from the queue.
func (p *JobProcessor) Start(ctx context.Context) {
	log.Printf("Worker %s starting job processor with concurrency: %d", p.workerID, p.maxConcurrency)

	jobsChan, err := p.queue.ConsumeJobs()
	if err != nil {
		log.Printf("Failed to consume jobs: %v", err)
		return
	}

	for {
		select {
		case job, ok := <-jobsChan:
			if !ok {
				log.Printf("Worker %s: jobs channel closed", p.workerID)
				return
			}

			// Acquire semaphore slot
			p.semaphore <- struct{}{}
			p.wg.Add(1)

			// Process job in goroutine
			go p.processJob(job)

		case <-ctx.Done():
			log.Printf("Worker %s: context cancelled, waiting for jobs to complete...", p.workerID)
			p.wg.Wait()
			log.Printf("Worker %s: all jobs completed", p.workerID)
			return
		}
	}
}

// processJob processes a single validation job.
func (p *JobProcessor) processJob(job shared.ValidationJob) {
	defer func() {
		<-p.semaphore // Release semaphore slot
		p.wg.Done()
	}()

	startTime := time.Now()
	log.Printf("Worker %s processing job %s for email: %s", p.workerID, job.JobID, job.Email)

	// Perform the validation
	result := p.validator.ValidateEmail(job.Email)

	// Set job ID in result
	result.JobID = job.JobID

	duration := time.Since(startTime)
	log.Printf("Worker %s completed job %s for email %s in %v: %s (%s)",
		p.workerID, job.JobID, job.Email, duration, result.Status, result.Reason)

	// Send result back to controller
	if err := p.queue.PublishResult(result); err != nil {
		log.Printf("Worker %s failed to publish result for job %s: %v", p.workerID, job.JobID, err)

		// Retry once after a short delay
		time.Sleep(1 * time.Second)
		if err := p.queue.PublishResult(result); err != nil {
			log.Printf("Worker %s failed to publish result for job %s after retry: %v",
				p.workerID, job.JobID, err)
		}
	}
}

// GetStats returns processing statistics.
func (p *JobProcessor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"worker_id":       p.workerID,
		"max_concurrency": p.maxConcurrency,
		"active_jobs":     len(p.semaphore),
		"available_slots": cap(p.semaphore) - len(p.semaphore),
	}
}
