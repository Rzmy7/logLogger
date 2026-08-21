package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/go-chi/chi/v5/middleware"
)

// LogPayload represents an incoming log ingestion request.
type LogPayload struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id,omitempty"`
	IP        string `json:"ip,omitempty"`
}

// ValidationErrorDetail provides details on a specific validation failure.
type ValidationErrorDetail struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ResponseMeta contains metadata returned in HTTP API responses.
type ResponseMeta struct {
	RequestID string `json:"request_id"`
	Timestamp string `json:"timestamp"`
}

// ErrorResponse conforms to the API error contract.
type ErrorResponse struct {
	Error struct {
		Code    string                  `json:"code"`
		Message string                  `json:"message"`
		Details []ValidationErrorDetail `json:"details,omitempty"`
	} `json:"error"`
	Meta ResponseMeta `json:"meta"`
}

// IngestSuccessData contains the data object for 202 Accepted responses.
type IngestSuccessData struct {
	Status    string `json:"status"`
	TraceID   string `json:"trace_id,omitempty"`
	RequestID string `json:"request_id"`
}

// IngestSuccessResponse conforms to the API 202 Accepted response format.
type IngestSuccessResponse struct {
	Data IngestSuccessData `json:"data"`
	Meta ResponseMeta      `json:"meta"`
}

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	producer kafka.Producer
}

// NewHandler creates a new Handler instance.
func NewHandler(producer kafka.Producer) *Handler {
	return &Handler{
		producer: producer,
	}
}

// HealthCheck godoc
// @Summary      Health check
// @Description  Check health status of the Ingestor service
// @Tags         health
// @Produce      json
// @Success      200  {object}  map[string]string
// @Router       /health [get]
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// IngestLogs godoc
// @Summary      Ingest log message
// @Description  Accept and queue a log entry into Kafka for downstream processing
// @Tags         logs
// @Accept       json
// @Produce      json
// @Param        X-Request-ID  header    string                 false  "Optional client-generated request ID"
// @Param        payload       body      LogPayload             true   "Log payload to ingest"
// @Success      202           {object}  IngestSuccessResponse  "Log queued successfully"
// @Failure      400           {object}  ErrorResponse          "Validation error or malformed JSON"
// @Failure      503           {object}  ErrorResponse          "Kafka queue unavailable"
// @Router       /api/v1/logs [post]
func (h *Handler) IngestLogs(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetReqID(r.Context())
	if reqID == "" {
		reqID = r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = fmt.Sprintf("req_%d", time.Now().UnixNano())
		}
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)

	var payload LogPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error: struct {
				Code    string                  `json:"code"`
				Message string                  `json:"message"`
				Details []ValidationErrorDetail `json:"details,omitempty"`
			}{
				Code:    "MALFORMED_JSON",
				Message: "Invalid JSON in request body",
			},
			Meta: ResponseMeta{
				RequestID: reqID,
				Timestamp: nowStr,
			},
		})
		return
	}

	valErrors := validateLogPayload(&payload)
	if len(valErrors) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error: struct {
				Code    string                  `json:"code"`
				Message string                  `json:"message"`
				Details []ValidationErrorDetail `json:"details,omitempty"`
			}{
				Code:    "VALIDATION_ERROR",
				Message: "Validation failed",
				Details: valErrors,
			},
			Meta: ResponseMeta{
				RequestID: reqID,
				Timestamp: nowStr,
			},
		})
		return
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error: struct {
				Code    string                  `json:"code"`
				Message string                  `json:"message"`
				Details []ValidationErrorDetail `json:"details,omitempty"`
			}{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to serialize log message",
			},
			Meta: ResponseMeta{
				RequestID: reqID,
				Timestamp: nowStr,
			},
		})
		return
	}

	key := payload.TraceID
	if key == "" {
		key = payload.Service
	}

	if err := h.producer.Publish(r.Context(), key, payloadBytes); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error: struct {
				Code    string                  `json:"code"`
				Message string                  `json:"message"`
				Details []ValidationErrorDetail `json:"details,omitempty"`
			}{
				Code:    "KAFKA_UNAVAILABLE",
				Message: "Failed to publish message to queue",
			},
			Meta: ResponseMeta{
				RequestID: reqID,
				Timestamp: nowStr,
			},
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(IngestSuccessResponse{
		Data: IngestSuccessData{
			Status:    "queued",
			TraceID:   payload.TraceID,
			RequestID: reqID,
		},
		Meta: ResponseMeta{
			RequestID: reqID,
			Timestamp: nowStr,
		},
	})
}

func validateLogPayload(p *LogPayload) []ValidationErrorDetail {
	var details []ValidationErrorDetail

	if strings.TrimSpace(p.Timestamp) == "" {
		details = append(details, ValidationErrorDetail{
			Field:   "timestamp",
			Message: "timestamp is required",
		})
	} else if _, err := time.Parse(time.RFC3339, p.Timestamp); err != nil {
		if _, errNano := time.Parse(time.RFC3339Nano, p.Timestamp); errNano != nil {
			details = append(details, ValidationErrorDetail{
				Field:   "timestamp",
				Message: "timestamp must be valid RFC3339 format",
			})
		}
	}

	validLevels := map[string]bool{
		"DEBUG": true,
		"INFO":  true,
		"WARN":  true,
		"ERROR": true,
		"FATAL": true,
	}
	levelUpper := strings.ToUpper(strings.TrimSpace(p.Level))
	if levelUpper == "" || !validLevels[levelUpper] {
		details = append(details, ValidationErrorDetail{
			Field:   "level",
			Message: "must be one of: DEBUG, INFO, WARN, ERROR, FATAL",
		})
	} else {
		p.Level = levelUpper
	}

	if strings.TrimSpace(p.Service) == "" {
		details = append(details, ValidationErrorDetail{
			Field:   "service",
			Message: "service is required",
		})
	}

	msgLen := len(strings.TrimSpace(p.Message))
	if msgLen < 1 || msgLen > 1000 {
		details = append(details, ValidationErrorDetail{
			Field:   "message",
			Message: "message must be between 1 and 1000 characters",
		})
	}

	if p.IP != "" {
		if net.ParseIP(p.IP) == nil {
			details = append(details, ValidationErrorDetail{
				Field:   "ip",
				Message: "must be a valid IPv4 or IPv6 address",
			})
		}
	}

	return details
}
