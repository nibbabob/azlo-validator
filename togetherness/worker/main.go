package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	shared "github.com/nibbabob/azlo-validator-shared"
)

func main() {
	// Get worker configuration from environment variables
	workerID := getEnv("WORKER_ID", "worker-1")
	maxConcurrency := getEnvInt("MAX_CONCURRENCY", 10)
	queueURL := getEnv("QUEUE_URL", "memory://localhost") // For future Redis/RabbitMQ support

	log.Printf("Starting worker %s with max concurrency: %d", workerID, maxConcurrency)

	// Initialize the queue connection
	// For now, we'll use a simple approach where workers connect to the controller
	// In production, this would connect to Redis, RabbitMQ, etc.
	queue := NewWorkerQueue(queueURL)
	defer queue.Close()

	// Create validator instance
	validator := shared.NewValidator()

	// Create job processor
	processor := NewJobProcessor(workerID, validator, queue, maxConcurrency)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the processor
	go processor.Start(ctx)

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("Worker %s shutting down...", workerID)
	cancel()

	// Give some time for graceful shutdown
	time.Sleep(5 * time.Second)
	log.Printf("Worker %s exited", workerID)
}

// getEnv gets an environment variable with a default value.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvInt gets an environment variable as an integer with a default value.
func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
