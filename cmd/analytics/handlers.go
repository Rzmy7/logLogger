package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/redis"
	"github.com/go-chi/chi/v5/middleware"
)

// ResponseMeta holds standard metadata returned across all API responses.
type ResponseMeta struct {
	RequestID string `json:"request_id"`
	Timestamp string `json:"timestamp"`
}

// ErrorDetail provides field-level error info.
type ErrorDetail struct {
	Field    string `json:"field,omitempty"`
	Expected string `json:"expected,omitempty"`
	Received string `json:"received,omitempty"`
	Message  string `json:"message,omitempty"`
}

// APIErrorResponse standard error payload.
type APIErrorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details,omitempty"`
	} `json:"error"`
	Meta ResponseMeta `json:"meta"`
}

// Handler holds dependencies for the Analytics API.
type Handler struct {
	redisClient redis.MetricsReader
	esClient    elastic.Searcher
}

// NewHandler creates a new Analytics API Handler.
func NewHandler(redisClient redis.MetricsReader, esClient elastic.Searcher) *Handler {
	return &Handler{
		redisClient: redisClient,
		esClient:    esClient,
	}
}

func getMeta(r *http.Request) ResponseMeta {
	reqID := middleware.GetReqID(r.Context())
	if reqID == "" {
		reqID = r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = fmt.Sprintf("req_%d", time.Now().UnixNano())
		}
	}
	return ResponseMeta{
		RequestID: reqID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details any) {
	writeJSON(w, status, APIErrorResponse{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Details any    `json:"details,omitempty"`
		}{
			Code:    code,
			Message: message,
			Details: details,
		},
		Meta: getMeta(r),
	})
}

// HealthCheck handles GET /health.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	services := map[string]string{
		"redis":         "up",
		"elasticsearch": "up",
	}

	status := "healthy"
	httpStatus := http.StatusOK

	if err := h.redisClient.Ping(ctx); err != nil {
		services["redis"] = "down"
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	if err := h.esClient.Ping(ctx); err != nil {
		services["elasticsearch"] = "down"
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	writeJSON(w, httpStatus, map[string]any{
		"data": map[string]any{
			"status":   status,
			"services": services,
		},
		"meta": getMeta(r),
	})
}

// LiveMetrics handles GET /metrics/live.
func (h *Handler) LiveMetrics(w http.ResponseWriter, r *http.Request) {
	servicesParam := r.URL.Query().Get("services")
	var requestedServices []string
	if servicesParam != "" && servicesParam != "all" {
		for _, s := range strings.Split(servicesParam, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				requestedServices = append(requestedServices, s)
			}
		}
	}

	totalLogs, servicesMap, err := h.redisClient.GetLiveMetrics(r.Context(), requestedServices)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve live metrics", nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"total_logs": totalLogs,
			"services":   servicesMap,
		},
		"meta": getMeta(r),
	})
}

// TopErrors handles GET /metrics/top-errors.
func (h *Handler) TopErrors(w http.ResponseWriter, r *http.Request) {
	n := 5
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		if val, err := strconv.Atoi(nStr); err == nil && val > 0 {
			n = val
		}
	}

	topErrors, err := h.redisClient.GetTopErrors(r.Context(), n)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve top errors", nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"top_errors": topErrors,
		},
		"meta": getMeta(r),
	})
}

// TopServices handles GET /metrics/top-services.
func (h *Handler) TopServices(w http.ResponseWriter, r *http.Request) {
	n := 5
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		if val, err := strconv.Atoi(nStr); err == nil && val > 0 {
			n = val
		}
	}

	topServices, err := h.redisClient.GetTopServices(r.Context(), n)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve top services", nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"top_services": topServices,
		},
		"meta": getMeta(r),
	})
}

// Search handles GET /search.
func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	page := 1
	if pageStr := query.Get("page"); pageStr != "" {
		if val, err := strconv.Atoi(pageStr); err == nil && val > 0 {
			page = val
		}
	}

	size := 20
	if sizeStr := query.Get("size"); sizeStr != "" {
		if val, err := strconv.Atoi(sizeStr); err == nil && val > 0 {
			size = val
		}
	}

	fromStr := query.Get("from")
	if fromStr != "" {
		if _, err := time.Parse(time.RFC3339, fromStr); err != nil {
			if _, errNano := time.Parse(time.RFC3339Nano, fromStr); errNano != nil {
				writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid date format", ErrorDetail{
					Field:    "from",
					Expected: "RFC3339",
					Received: fromStr,
				})
				return
			}
		}
	}

	toStr := query.Get("to")
	if toStr != "" {
		if _, err := time.Parse(time.RFC3339, toStr); err != nil {
			if _, errNano := time.Parse(time.RFC3339Nano, toStr); errNano != nil {
				writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid date format", ErrorDetail{
					Field:    "to",
					Expected: "RFC3339",
					Received: toStr,
				})
				return
			}
		}
	}

	params := elastic.SearchParams{
		Query:   query.Get("q"),
		Service: query.Get("service"),
		Level:   query.Get("level"),
		TraceID: query.Get("trace_id"),
		From:    fromStr,
		To:      toStr,
		Page:    page,
		Size:    size,
	}

	searchResult, err := h.esClient.SearchLogs(r.Context(), params)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "SEARCH_ERROR", fmt.Sprintf("Search query failed: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": searchResult,
		"meta": getMeta(r),
	})
}
