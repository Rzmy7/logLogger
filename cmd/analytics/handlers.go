package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/redis"
	"github.com/Rzmy7/logLogger/internal/retention"
	"github.com/go-chi/chi/v5"
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
	redisClient      redis.MetricsReader
	esClient         elastic.Searcher
	retentionManager retention.Manager
}

// NewHandler creates a new Analytics API Handler.
func NewHandler(redisClient redis.MetricsReader, esClient elastic.Searcher, retentionManager retention.Manager) *Handler {
	return &Handler{
		redisClient:      redisClient,
		esClient:         esClient,
		retentionManager: retentionManager,
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
	resp := APIErrorResponse{
		Meta: getMeta(r),
	}
	resp.Error.Code = code
	resp.Error.Message = message
	resp.Error.Details = details

	writeJSON(w, status, resp)
}

// HealthCheck verifies availability of dependencies (Elasticsearch, Redis).
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	redisCtx, redisCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer redisCancel()

	redisStatus := "healthy"
	if err := h.redisClient.Ping(redisCtx); err != nil {
		redisStatus = fmt.Sprintf("unreachable: %v", err)
	}

	esCtx, esCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer esCancel()

	esStatus := "healthy"
	if err := h.esClient.Ping(esCtx); err != nil {
		esStatus = fmt.Sprintf("unreachable: %v", err)
	}

	overallStatus := "healthy"
	httpStatus := http.StatusOK
	if redisStatus != "healthy" || esStatus != "healthy" {
		overallStatus = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	writeJSON(w, httpStatus, map[string]any{
		"status": overallStatus,
		"dependencies": map[string]string{
			"redis":         redisStatus,
			"elasticsearch": esStatus,
		},
		"meta": getMeta(r),
	})
}

// LiveMetrics returns real-time log counts and per-service error counts.
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

	totalLogs, serviceMetrics, err := h.redisClient.GetLiveMetrics(r.Context(), requestedServices)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", fmt.Sprintf("Failed to query live metrics: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"total_logs": totalLogs,
			"services":   serviceMetrics,
		},
		"meta": getMeta(r),
	})
}

// TopErrors returns top ranked error messages from Redis sorted set.
func (h *Handler) TopErrors(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	n := 10
	if nStr != "" {
		parsed, err := strconv.Atoi(nStr)
		if err != nil || parsed <= 0 {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Query param 'n' must be a positive integer", ErrorDetail{
				Field:    "n",
				Expected: "positive integer",
				Received: nStr,
			})
			return
		}
		n = parsed
	}

	topErrors, err := h.redisClient.GetTopErrors(r.Context(), n)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", fmt.Sprintf("Failed to query top errors: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": topErrors,
		"meta": getMeta(r),
	})
}

// TopServices returns service rankings by log volume from Redis sorted set.
func (h *Handler) TopServices(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	n := 10
	if nStr != "" {
		parsed, err := strconv.Atoi(nStr)
		if err != nil || parsed <= 0 {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Query param 'n' must be a positive integer", ErrorDetail{
				Field:    "n",
				Expected: "positive integer",
				Received: nStr,
			})
			return
		}
		n = parsed
	}

	topServices, err := h.redisClient.GetTopServices(r.Context(), n)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", fmt.Sprintf("Failed to query top services: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": topServices,
		"meta": getMeta(r),
	})
}

// Search executes full-text search against Elasticsearch with filtering and pagination.
func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	page := 1
	if pageStr := query.Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	size := 20
	if sizeStr := query.Get("size"); sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
			size = s
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

	searchQuery := query.Get("q")
	if searchQuery == "" {
		searchQuery = query.Get("query")
	}

	params := elastic.SearchParams{
		TenantID: query.Get("tenant_id"),
		Query:    searchQuery,
		Service:  query.Get("service"),
		Level:    query.Get("level"),
		TraceID:  query.Get("trace_id"),
		From:     fromStr,
		To:       toStr,
		Page:     page,
		Size:     size,
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

// DeleteIndex handles administrative request DELETE /admin/logs/indices/{index}.
func (h *Handler) DeleteIndex(w http.ResponseWriter, r *http.Request) {
	indexName := chi.URLParam(r, "index")
	if strings.TrimSpace(indexName) == "" {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Index name parameter is required", nil)
		return
	}

	err := h.retentionManager.DeleteIndexByName(r.Context(), indexName)
	if err != nil {
		if errors.Is(err, retention.ErrInvalidIndexName) {
			writeError(w, r, http.StatusBadRequest, "INVALID_INDEX_NAME", err.Error(), ErrorDetail{
				Field:    "index",
				Expected: "logs-v1-YYYY.MM.DD",
				Received: indexName,
			})
			return
		}
		if errors.Is(err, retention.ErrProtectedIndex) {
			writeError(w, r, http.StatusUnprocessableEntity, "PROTECTED_INDEX", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("Index %q not found", indexName), nil)
			return
		}
		writeError(w, r, http.StatusInternalServerError, "DELETE_ERROR", fmt.Sprintf("Failed to delete index: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]string{
			"deleted_index": indexName,
			"status":        "deleted",
		},
		"meta": getMeta(r),
	})
}

// DeleteLogsBefore handles administrative request DELETE /admin/logs?before=<RFC3339>.
func (h *Handler) DeleteLogsBefore(w http.ResponseWriter, r *http.Request) {
	beforeStr := r.URL.Query().Get("before")
	if beforeStr == "" {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Query parameter 'before' is required", ErrorDetail{
			Field:    "before",
			Expected: "RFC3339 timestamp",
			Received: "",
		})
		return
	}

	beforeTime, err := time.Parse(time.RFC3339, beforeStr)
	if err != nil {
		if beforeTime, err = time.Parse(time.RFC3339Nano, beforeStr); err != nil {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid date format for 'before'", ErrorDetail{
				Field:    "before",
				Expected: "RFC3339 timestamp",
				Received: beforeStr,
			})
			return
		}
	}

	result, err := h.retentionManager.DeleteIndicesBefore(r.Context(), beforeTime)
	if err != nil {
		if errors.Is(err, retention.ErrFutureCutoff) {
			writeError(w, r, http.StatusBadRequest, "INVALID_CUTOFF", "Cutoff timestamp cannot be in the future", nil)
			return
		}
		writeError(w, r, http.StatusInternalServerError, "DELETE_ERROR", fmt.Sprintf("Failed to delete indices: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": getMeta(r),
	})
}

// GetLogStats handles administrative request GET /admin/logs/stats.
func (h *Handler) GetLogStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.retentionManager.GetStats(r.Context())
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "STATS_ERROR", fmt.Sprintf("Failed to query log storage stats: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": stats,
		"meta": getMeta(r),
	})
}

// RunRetention handles administrative manual trigger POST /admin/logs/retention/run?days=30.
func (h *Handler) RunRetention(w http.ResponseWriter, r *http.Request) {
	days := 30
	if daysStr := r.URL.Query().Get("days"); daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil && d > 0 {
			days = d
		} else {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Query parameter 'days' must be a positive integer", ErrorDetail{
				Field:    "days",
				Expected: "positive integer",
				Received: daysStr,
			})
			return
		}
	}

	result, err := h.retentionManager.RunRetention(r.Context(), days)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "RETENTION_ERROR", fmt.Sprintf("Retention run failed: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": getMeta(r),
	})
}
