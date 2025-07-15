package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	shared "github.com/nibbabob/azlo-validator-shared"
)

func main() {
	// Get configuration from environment variables
	queueType := getEnv("QUEUE_TYPE", "memory") // "memory" or "redis"
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	port := getEnv("PORT", "8080")

	// Initialize the queue based on configuration
	var queue shared.Queue

	switch queueType {
	case "redis":
		log.Printf("Using Redis queue at %s", redisAddr)
		queue = NewRedisQueue(redisAddr)
	case "memory":
		log.Printf("Using in-memory queue")
		queue = NewInMemoryQueue()
	default:
		log.Printf("Unknown queue type '%s', defaulting to in-memory", queueType)
		queue = NewInMemoryQueue()
	}

	defer queue.Close()

	// Initialize the API handler
	apiHandler := NewAPIHandler(queue)

	// Set up HTTP router
	r := mux.NewRouter()

	// API routes
	r.HandleFunc("/api/v1/validate", apiHandler.ValidateSingle).Methods("POST")
	r.HandleFunc("/api/v1/validate/batch", apiHandler.ValidateBatch).Methods("POST")
	r.HandleFunc("/api/v1/status/{jobId}", apiHandler.GetStatus).Methods("GET")
	r.HandleFunc("/api/v1/result/{jobId}", apiHandler.GetResult).Methods("GET")

	// Health check
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	// Queue stats endpoint (useful for monitoring)
	r.HandleFunc("/api/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		stats := map[string]interface{}{
			"queue_type": queueType,
		}

		// Add queue-specific stats if available
		if redisQueue, ok := queue.(*RedisQueue); ok {
			queueStats := redisQueue.GetQueueStats()
			for k, v := range queueStats {
				stats[k] = v
			}
		}

		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "Failed to encode stats", http.StatusInternalServerError)
		}
	}).Methods("GET")

	// Create HTTP server
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start result processor in background
	go apiHandler.ProcessResults(ctx)

	// Start server in a goroutine
	go func() {
		log.Printf("Controller server starting on port %s", port)
		log.Printf("Using queue type: %s", queueType)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Cancel context to stop background processes
	cancel()

	// Give outstanding requests 30 seconds to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

// getEnv gets an environment variable with a default value.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
