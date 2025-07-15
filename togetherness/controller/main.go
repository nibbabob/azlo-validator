package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	// Initialize the queue (using in-memory queue for this example)
	queue := NewInMemoryQueue()
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

	// Create HTTP server
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start result processor in background
	go apiHandler.ProcessResults(context.Background())

	// Start server in a goroutine
	go func() {
		log.Printf("Controller server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Give outstanding requests 30 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
