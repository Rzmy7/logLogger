package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/retention"
)

// DefaultAPIURL is the fallback URL for the Analytics API.
const DefaultAPIURL = "http://localhost:8082"

// APIError represents a structured error returned by the Analytics API.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Details    any
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("API error (%d %s): %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("API error (%d): %s", e.StatusCode, e.Message)
}

// RawAPIErrorResponse matches the Analytics API error payload structure.
type RawAPIErrorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details,omitempty"`
	} `json:"error"`
}

// HealthResponse represents the health check response from Analytics API.
type HealthResponse struct {
	Status       string            `json:"status"`
	Dependencies map[string]string `json:"dependencies"`
	Meta         struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// SearchResponse represents the response from GET /search.
type SearchResponse struct {
	Data elastic.SearchResult `json:"data"`
	Meta struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// LogStatsResponse represents the response from GET /admin/logs/stats.
type LogStatsResponse struct {
	Data elastic.LogStats `json:"data"`
	Meta struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// DeleteIndexResponse represents the response from DELETE /admin/logs/indices/{index}.
type DeleteIndexResponse struct {
	Data struct {
		Index  string `json:"index"`
		Status string `json:"status"`
	} `json:"data"`
	Meta struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// DeleteBeforeResponse represents the response from DELETE /admin/logs?before=...
type DeleteBeforeResponse struct {
	Data retention.RetentionResult `json:"data"`
	Meta struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// RetentionRunResponse represents the response from POST /admin/logs/retention/run.
type RetentionRunResponse struct {
	Data retention.RetentionResult `json:"data"`
	Meta struct {
		RequestID string `json:"request_id"`
		Timestamp string `json:"timestamp"`
	} `json:"meta"`
}

// Client is an HTTP client for the Analytics & Admin API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithBaseURL sets a custom base URL.
func WithBaseURL(rawURL string) Option {
	return func(c *Client) {
		if rawURL != "" {
			c.baseURL = strings.TrimRight(rawURL, "/")
		}
	}
}

// NewClient creates a new API client. Base URL defaults to LOGCTL_API_URL env or http://localhost:8082.
func NewClient(opts ...Option) *Client {
	apiURL := os.Getenv("LOGCTL_API_URL")
	if apiURL == "" {
		apiURL = DefaultAPIURL
	}

	c := &Client{
		baseURL: strings.TrimRight(apiURL, "/"),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// BaseURL returns the configured base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}

func (c *Client) execute(ctx context.Context, req *http.Request, target any) error {
	req = req.WithContext(ctx)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "logctl/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return fmt.Errorf("request canceled: %w", ctx.Err())
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("request timed out: %w", ctx.Err())
		}
		return fmt.Errorf("failed to connect to Analytics API at %s: %w", c.baseURL, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var rawErr RawAPIErrorResponse
		if jsonErr := json.Unmarshal(bodyBytes, &rawErr); jsonErr == nil && (rawErr.Error.Message != "" || rawErr.Error.Code != "") {
			return &APIError{
				StatusCode: resp.StatusCode,
				Code:       rawErr.Error.Code,
				Message:    rawErr.Error.Message,
				Details:    rawErr.Error.Details,
			}
		}
		msg := strings.TrimSpace(string(bodyBytes))
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    msg,
		}
	}

	if target != nil {
		if len(bodyBytes) == 0 {
			return errors.New("empty response body from API")
		}
		if err := json.Unmarshal(bodyBytes, target); err != nil {
			return fmt.Errorf("failed to decode JSON response: %w", err)
		}
	}

	return nil
}

// Health checks the Analytics API health endpoint (GET /health).
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp HealthResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetLogStats retrieves administrative storage stats (GET /admin/logs/stats).
func (c *Client) GetLogStats(ctx context.Context) (*LogStatsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/admin/logs/stats", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp LogStatsResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SearchParams contains filter parameters for SearchLogs.
type SearchParams struct {
	Query    string
	Service  string
	Level    string
	TraceID  string
	TenantID string
	From     string
	To       string
	Page     int
	Size     int
}

// SearchLogs queries the log store (GET /search?...).
func (c *Client) SearchLogs(ctx context.Context, params SearchParams) (*SearchResponse, error) {
	q := url.Values{}
	if params.Query != "" {
		q.Set("q", params.Query)
	}
	if params.Service != "" {
		q.Set("service", params.Service)
	}
	if params.Level != "" {
		q.Set("level", params.Level)
	}
	if params.TraceID != "" {
		q.Set("trace_id", params.TraceID)
	}
	if params.TenantID != "" {
		q.Set("tenant_id", params.TenantID)
	}
	if params.From != "" {
		q.Set("from", params.From)
	}
	if params.To != "" {
		q.Set("to", params.To)
	}
	if params.Page > 0 {
		q.Set("page", strconv.Itoa(params.Page))
	}
	if params.Size > 0 {
		q.Set("size", strconv.Itoa(params.Size))
	}

	endpoint := c.baseURL + "/search"
	if encoded := q.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp SearchResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteIndex deletes a specific index by name (DELETE /admin/logs/indices/{index}).
func (c *Client) DeleteIndex(ctx context.Context, indexName string) (*DeleteIndexResponse, error) {
	if strings.TrimSpace(indexName) == "" {
		return nil, errors.New("index name cannot be empty")
	}

	escapedIndex := url.PathEscape(indexName)
	endpoint := fmt.Sprintf("%s/admin/logs/indices/%s", c.baseURL, escapedIndex)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp DeleteIndexResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteBefore deletes all indices older than the specified RFC3339 timestamp (DELETE /admin/logs?before=...).
func (c *Client) DeleteBefore(ctx context.Context, before string) (*DeleteBeforeResponse, error) {
	if strings.TrimSpace(before) == "" {
		return nil, errors.New("cutoff timestamp 'before' cannot be empty")
	}

	q := url.Values{}
	q.Set("before", before)
	endpoint := fmt.Sprintf("%s/admin/logs?%s", c.baseURL, q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp DeleteBeforeResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// RunRetention triggers manual retention policy enforcement (POST /admin/logs/retention/run?days=...).
func (c *Client) RunRetention(ctx context.Context, days int) (*RetentionRunResponse, error) {
	endpoint := c.baseURL + "/admin/logs/retention/run"
	if days > 0 {
		q := url.Values{}
		q.Set("days", strconv.Itoa(days))
		endpoint += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp RetentionRunResponse
	if err := c.execute(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
