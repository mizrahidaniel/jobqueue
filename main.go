package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	db, err := initDB("jobqueue.db")
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	server := &Server{db: db}
	router := mux.NewRouter()

	// Routes
	router.HandleFunc("/jobs", server.EnqueueJob).Methods("POST")
	router.HandleFunc("/jobs/next", server.FetchNextJob).Methods("GET")
	router.HandleFunc("/jobs/{id}/complete", server.CompleteJob).Methods("POST")
	router.HandleFunc("/jobs/{id}/fail", server.FailJob).Methods("POST")
	router.HandleFunc("/jobs/{id}", server.GetJob).Methods("GET")
	router.HandleFunc("/health", server.Health).Methods("GET")

	log.Printf("🚀 JobQueue server starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}

type Server struct {
	db *DB
}

type Job struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Payload        map[string]interface{} `json:"payload"`
	Status         string                 `json:"status"`
	MaxRetries     int                    `json:"max_retries"`
	Attempts       int                    `json:"attempts"`
	TimeoutSeconds int                    `json:"timeout_seconds"`
	Priority       int                    `json:"priority"`
	Error          string                 `json:"error,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
}

type EnqueueRequest struct {
	Type           string                 `json:"type"`
	Payload        map[string]interface{} `json:"payload"`
	MaxRetries     int                    `json:"max_retries"`
	TimeoutSeconds int                    `json:"timeout_seconds"`
	Priority       int                    `json:"priority"`
}

// EnqueueJob - POST /jobs
func (s *Server) EnqueueJob(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, "Job type is required", http.StatusBadRequest)
		return
	}

	// Defaults
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = 300
	}

	job, err := s.db.EnqueueJob(req.Type, req.Payload, req.MaxRetries, req.TimeoutSeconds, req.Priority)
	if err != nil {
		log.Printf("Failed to enqueue job: %v", err)
		http.Error(w, "Failed to enqueue job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}

// FetchNextJob - GET /jobs/next?type=ml_inference&wait=30s
func (s *Server) FetchNextJob(w http.ResponseWriter, r *http.Request) {
	jobType := r.URL.Query().Get("type")
	if jobType == "" {
		http.Error(w, "Job type is required", http.StatusBadRequest)
		return
	}

	waitParam := r.URL.Query().Get("wait")
	waitDuration := 30 * time.Second
	if waitParam != "" {
		if d, err := time.ParseDuration(waitParam); err == nil {
			waitDuration = d
		}
	}

	// Long-polling: poll for job up to waitDuration
	timeout := time.After(waitDuration)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-ticker.C:
			job, err := s.db.FetchNextJob(jobType)
			if err != nil {
				log.Printf("Failed to fetch next job: %v", err)
				continue
			}
			if job != nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(job)
				return
			}
		}
	}
}

// CompleteJob - POST /jobs/:id/complete
func (s *Server) CompleteJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	if err := s.db.CompleteJob(jobID); err != nil {
		log.Printf("Failed to complete job %s: %v", jobID, err)
		http.Error(w, "Failed to complete job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Job %s completed", jobID)
}

// FailJob - POST /jobs/:id/fail?error=timeout
func (s *Server) FailJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]
	errorMsg := r.URL.Query().Get("error")

	if err := s.db.FailJob(jobID, errorMsg); err != nil {
		log.Printf("Failed to mark job %s as failed: %v", jobID, err)
		http.Error(w, "Failed to mark job as failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Job %s marked as failed", jobID)
}

// GetJob - GET /jobs/:id
func (s *Server) GetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	job, err := s.db.GetJob(jobID)
	if err != nil {
		log.Printf("Failed to get job %s: %v", jobID, err)
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// Health - GET /health
func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	stats, _ := s.db.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"stats":  stats,
	})
}
