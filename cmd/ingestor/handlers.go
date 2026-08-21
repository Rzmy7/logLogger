package main

import (
	"net/http"
)

// Handler holds dependencies for HTTP handlers (e.g. log queue, database, logger).
type Handler struct {
	// Dependencies will be added here (e.g., kafka producer, redis client, logger)
}

// NewHandler creates a new Handler instance.
func NewHandler() *Handler {
	return &Handler{}
}

// HealthCheck handles GET /health requests.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// IngestLogs handles POST /api/v1/logs requests.
func (h *Handler) IngestLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message":"logs received"}`))
}
