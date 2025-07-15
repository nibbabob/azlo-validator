// File: worker/processor_enhanced.go
package main

import (
	"context"
	"log"
	"sync"
	"time"

	shared "github.com/nibbabob/azlo-validator-shared"
)

// EnhancedJobProcessor handles validation jobs with IP reputation checking
type EnhancedJobProcessor struct {
	workerID       string
	validator      *shared.EnhancedValidator
	queue          WorkerQueue
	maxConcurrency int
	semaphore      chan struct{}
	wg             sync.WaitGroup
}

// NewEnhancedJobProcessor creates a new enhanced job processor
func NewEnhancedJobProcessor(workerID string, validator *shared.EnhancedValidator, queue WorkerQueue, maxConcurrency int) *EnhancedJobProcessor {
	return &EnhancedJobProcessor{
		workerID:       workerID,
		validator:      validator,
		queue:          queue,
		maxConcurrency: maxConcurrency,
		semaphore:      make(chan struct{}, maxConcurrency),
	}
}

// Start begins processing jobs from the queue
func (p *EnhancedJobProcessor) Start(ctx context.Context) {
	log.Printf("Enhanced Worker %s starting job processor with concurrency: %d", p.workerID, p.maxConcurrency)

	jobsChan, err := p.queue.ConsumeJobs()
	if err != nil {
		log.Printf("Failed to consume jobs: %v", err)
		return
	}

	// Start cache cleanup routine
	go p.cacheCleanupRoutine(ctx)

	for {
		select {
		case job, ok := <-jobsChan:
			if !ok {
				log.Printf("Enhanced Worker %s: jobs channel closed", p.workerID)
				return
			}

			// Acquire semaphore slot
			p.semaphore <- struct{}{}
			p.wg.Add(1)

			// Process job in goroutine
			go p.processJobWithReputation(job)

		case <-ctx.Done():
			log.Printf("Enhanced Worker %s: context cancelled, waiting for jobs to complete...", p.workerID)
			p.wg.Wait()
			log.Printf("Enhanced Worker %s: all jobs completed", p.workerID)
			return
		}
	}
}

// processJobWithReputation processes a single validation job with IP reputation checking
func (p *EnhancedJobProcessor) processJobWithReputation(job shared.ValidationJob) {
	defer func() {
		<-p.semaphore // Release semaphore slot
		p.wg.Done()
	}()

	startTime := time.Now()
	log.Printf("Enhanced Worker %s processing job %s for email: %s", p.workerID, job.JobID, job.Email)

	// Perform the enhanced validation with IP reputation checking
	result := p.validator.ValidateEmailWithReputation(job.Email)

	// Set job ID in result
	result.JobID = job.JobID

	duration := time.Since(startTime)
	log.Printf("Enhanced Worker %s completed job %s for email %s in %v: %s (%s)",
		p.workerID, job.JobID, job.Email, duration, result.Status, result.Reason)

	// Send result back to controller
	if err := p.queue.PublishResult(*result); err != nil {
		log.Printf("Enhanced Worker %s failed to publish result for job %s: %v", p.workerID, job.JobID, err)

		// Retry once after a short delay
		time.Sleep(1 * time.Second)
		if err := p.queue.PublishResult(*result); err != nil {
			log.Printf("Enhanced Worker %s failed to publish result for job %s after retry: %v",
				p.workerID, job.JobID, err)
		}
	}
}

// cacheCleanupRoutine periodically cleans up expired cache entries
func (p *EnhancedJobProcessor) cacheCleanupRoutine(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.validator.ClearExpiredCache()
			log.Printf("Enhanced Worker %s: cleaned up expired IP reputation cache entries", p.workerID)
		case <-ctx.Done():
			return
		}
	}
}

// GetStats returns processing statistics including cache stats
func (p *EnhancedJobProcessor) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"worker_id":       p.workerID,
		"max_concurrency": p.maxConcurrency,
		"active_jobs":     len(p.semaphore),
		"available_slots": cap(p.semaphore) - len(p.semaphore),
	}

	// Add cache statistics
	cacheStats := p.validator.GetCacheStats()
	for k, v := range cacheStats {
		stats["cache_"+k] = v
	}

	return stats
}
