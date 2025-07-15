package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	shared "github.com/nibbabob/azlo-validator-shared"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// APIHandler handles HTTP requests and manages job processing.
type APIHandler struct {
	queue   shared.Queue
	results map[string]*shared.Result
	jobs    map[string]*JobStatus
	mutex   sync.RWMutex
}

// JobStatus tracks the status of validation jobs.
type JobStatus struct {
	JobID     string                    `json:"job_id"`
	Status    string                    `json:"status"`
	Emails    []string                  `json:"emails"`
	Results   map[string]*shared.Result `json:"results,omitempty"`
	CreatedAt time.Time                 `json:"created_at"`
	UpdatedAt time.Time                 `json:"updated_at"`
}

// SingleValidationRequest represents a single email validation request.
type SingleValidationRequest struct {
	Email string `json:"email"`
}

// NewAPIHandler creates a new API handler.
func NewAPIHandler(queue shared.Queue) *APIHandler {
	return &APIHandler{
		queue:   queue,
		results: make(map[string]*shared.Result),
		jobs:    make(map[string]*JobStatus),
	}
}

// ValidateSingle handles single email validation requests.
func (h *APIHandler) ValidateSingle(w http.ResponseWriter, r *http.Request) {
	var req SingleValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Email == "" {
		http.Error(w, "Email is required", http.StatusBadRequest)
		return
	}

	// Create job
	jobID := uuid.New().String()
	job := shared.ValidationJob{
		JobID:     jobID,
		Email:     req.Email,
		Timestamp: time.Now(),
	}

	// Store job status
	h.mutex.Lock()
	h.jobs[jobID] = &JobStatus{
		JobID:     jobID,
		Status:    "pending",
		Emails:    []string{req.Email},
		Results:   make(map[string]*shared.Result),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	h.mutex.Unlock()

	// Publish job to queue
	if err := h.queue.PublishJob(job); err != nil {
		log.Printf("Failed to publish job: %v", err)
		http.Error(w, "Failed to queue validation job", http.StatusInternalServerError)
		return
	}

	// Return job ID
	response := map[string]string{
		"job_id": jobID,
		"status": "pending",
		"email":  req.Email,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ValidateBatch handles batch email validation requests.
func (h *APIHandler) ValidateBatch(w http.ResponseWriter, r *http.Request) {
	var req shared.BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.Emails) == 0 {
		http.Error(w, "At least one email is required", http.StatusBadRequest)
		return
	}

	if len(req.Emails) > 1000 {
		http.Error(w, "Maximum 1000 emails per batch", http.StatusBadRequest)
		return
	}

	// Create batch job
	jobID := uuid.New().String()

	// Store job status
	h.mutex.Lock()
	h.jobs[jobID] = &JobStatus{
		JobID:     jobID,
		Status:    "pending",
		Emails:    req.Emails,
		Results:   make(map[string]*shared.Result),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	h.mutex.Unlock()

	// Create individual validation jobs for each email
	for _, email := range req.Emails {
		job := shared.ValidationJob{
			JobID:     jobID + "_" + email, // Unique job ID per email
			Email:     email,
			Timestamp: time.Now(),
		}

		if err := h.queue.PublishJob(job); err != nil {
			log.Printf("Failed to publish job for email %s: %v", email, err)
		}
	}

	// Return batch response
	response := shared.BatchResponse{
		JobID:   jobID,
		Emails:  req.Emails,
		Status:  "pending",
		Message: "Batch validation job created",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GetStatus returns the status of a validation job.
func (h *APIHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	h.mutex.RLock()
	jobStatus, exists := h.jobs[jobID]
	h.mutex.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobStatus)
}

// GetResult returns the result of a validation job.
func (h *APIHandler) GetResult(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	h.mutex.RLock()
	jobStatus, exists := h.jobs[jobID]
	h.mutex.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Check if job is complete
	if jobStatus.Status != "completed" && jobStatus.Status != "failed" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  jobStatus.Status,
			"message": "Job not yet completed",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobStatus.Results)
}

// ProcessResults processes validation results from workers.
func (h *APIHandler) ProcessResults(ctx context.Context) {
	resultsChan, err := h.queue.ConsumeResults()
	if err != nil {
		log.Printf("Failed to consume results: %v", err)
		return
	}

	for {
		select {
		case result := <-resultsChan:
			h.handleResult(result)
		case <-ctx.Done():
			log.Println("Result processor stopping...")
			return
		}
	}
}

// handleResult processes a single validation result.
func (h *APIHandler) handleResult(result shared.Result) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Store the result
	h.results[result.JobID] = &result

	// Update job status
	// For batch jobs, the JobID format is batchJobID_email
	var jobID string
	if len(result.JobID) > 36 && result.JobID[36] == '_' {
		jobID = result.JobID[:36] // Extract batch job ID
	} else {
		jobID = result.JobID
	}

	if jobStatus, exists := h.jobs[jobID]; exists {
		jobStatus.Results[result.Email] = &result
		jobStatus.UpdatedAt = time.Now()

		// Check if all emails in the job have been processed
		if len(jobStatus.Results) == len(jobStatus.Emails) {
			jobStatus.Status = "completed"
		} else {
			jobStatus.Status = "processing"
		}
	}

	log.Printf("Processed result for job %s, email %s: %s",
		result.JobID, result.Email, result.Status)
}
