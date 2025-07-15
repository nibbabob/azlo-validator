// File: worker/main_enhanced.go
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

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		// Try to convert the string to an integer
		intValue, err := strconv.Atoi(value)
		if err == nil {
			return intValue
		}
		// If conversion fails, return the default value
	}
	return defaultValue
}

func main() {
	// Get worker configuration from environment variables
	workerID := getEnv("WORKER_ID", "worker-1")
	maxConcurrency := getEnvInt("MAX_CONCURRENCY", 10)
	queueType := getEnv("QUEUE_TYPE", "memory")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	abuseIPDBKey := getEnv("ABUSEIPDB_API_KEY", "")

	if abuseIPDBKey == "" {
		log.Fatal("ABUSEIPDB_API_KEY environment variable is required")
	}

	log.Printf("Starting enhanced worker %s with max concurrency: %d", workerID, maxConcurrency)
	log.Printf("Queue type: %s", queueType)
	log.Printf("AbuseIPDB integration enabled")

	// Initialize the queue connection
	var queueURL string
	switch queueType {
	case "redis":
		queueURL = "redis://" + redisAddr
	case "memory":
		queueURL = "memory://localhost"
	default:
		log.Printf("Unknown queue type '%s', defaulting to memory", queueType)
		queueURL = "memory://localhost"
	}

	queue := NewWorkerQueue(queueURL)
	defer queue.Close()

	// Create enhanced validator instance with AbuseIPDB
	validator := shared.NewEnhancedValidator(abuseIPDBKey)

	// Create enhanced job processor
	processor := NewEnhancedJobProcessor(workerID, validator, queue, maxConcurrency)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the processor
	go processor.Start(ctx)

	// Log worker stats periodically (every 30 seconds)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := processor.GetStats()
				log.Printf("Enhanced worker stats: %+v", stats)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("Enhanced Worker %s shutting down...", workerID)
	cancel()

	// Give some time for graceful shutdown
	time.Sleep(5 * time.Second)
	log.Printf("Enhanced Worker %s exited", workerID)
}
