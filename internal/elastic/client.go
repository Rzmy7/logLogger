package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

const (
	TemplateName = "logs-v1-template"
	IndexPattern = "logs-v1-*"
)

// IndexTemplateDefinition holds the documented Elasticsearch index template JSON.
const IndexTemplateDefinition = `{
  "index_patterns": ["logs-v1-*"],
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "index.refresh_interval": "5s"
    },
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "timestamp": {
          "type": "date",
          "format": "strict_date_optional_time||epoch_millis"
        },
        "level": {
          "type": "keyword"
        },
        "service": {
          "type": "keyword"
        },
        "message": {
          "type": "text",
          "analyzer": "standard"
        },
        "trace_id": {
          "type": "keyword"
        },
        "ip": {
          "type": "ip"
        },
        "ingested_at": {
          "type": "date",
          "format": "strict_date_optional_time||epoch_millis"
        }
      }
    }
  }
}`

// SearchParams encapsulates query parameters for searching logs.
type SearchParams struct {
	Query   string
	Service string
	Level   string
	TraceID string
	From    string
	To      string
	Page    int
	Size    int
}

// SearchResult holds paginated log results.
type SearchResult struct {
	Total int64                 `json:"total"`
	Page  int                   `json:"page"`
	Size  int                   `json:"size"`
	Pages int                   `json:"pages"`
	Logs  []*models.LogDocument `json:"logs"`
}

// Indexer defines the contract for indexing documents into Elasticsearch.
type Indexer interface {
	EnsureTemplate(ctx context.Context) error
	IndexLog(ctx context.Context, logMsg *models.LogMessage, ingestedAt time.Time) error
	Ping(ctx context.Context) error
}

// Searcher defines the contract for querying logs from Elasticsearch.
type Searcher interface {
	SearchLogs(ctx context.Context, params SearchParams) (*SearchResult, error)
	Ping(ctx context.Context) error
}

// Client wraps Elasticsearch v8 client.
type Client struct {
	es *elasticsearch.Client
}

// NewClient initializes a new Elasticsearch client with the given URL.
func NewClient(esURL string) (*Client, error) {
	if strings.TrimSpace(esURL) == "" {
		return nil, errors.New("elasticsearch URL cannot be empty")
	}

	cfg := elasticsearch.Config{
		Addresses:     []string{esURL},
		MaxRetries:    3,
		RetryOnStatus: []int{502, 503, 504},
		RetryBackoff: func(attempt int) time.Duration {
			return time.Duration(attempt*100) * time.Millisecond
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	return &Client{es: es}, nil
}

// Ping checks if the Elasticsearch cluster is reachable.
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.es.Ping(c.es.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("elasticsearch ping error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping returned status: %s", res.Status())
	}
	return nil
}

// EnsureTemplate creates or updates the logs-v1-template index template with retry backoff.
func (c *Client) EnsureTemplate(ctx context.Context) error {
	var lastErr error

	for attempt := 1; attempt <= 3; attempt++ {
		req := strings.NewReader(IndexTemplateDefinition)
		res, err := c.es.Indices.PutIndexTemplate(
			TemplateName,
			req,
			c.es.Indices.PutIndexTemplate.WithContext(ctx),
		)
		if err == nil {
			defer res.Body.Close()
			if !res.IsError() {
				log.Printf("[INFO] Elasticsearch index template %q verified/created successfully", TemplateName)
				return nil
			}
			body, _ := io.ReadAll(res.Body)
			lastErr = fmt.Errorf("error creating index template %s: %s (%s)", TemplateName, res.Status(), string(body))
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
		}
	}

	return fmt.Errorf("failed to ensure template after retries: %w", lastErr)
}

// IndexLog indexes a single log document into the appropriate daily index (logs-v1-YYYY.MM.DD).
func (c *Client) IndexLog(ctx context.Context, logMsg *models.LogMessage, ingestedAt time.Time) error {
	if logMsg == nil {
		return errors.New("log message cannot be nil")
	}

	start := time.Now()
	defer func() {
		metrics.ElasticsearchIndexingDuration.Observe(time.Since(start).Seconds())
	}()

	t, err := logMsg.ParsedTime()
	if err != nil {
		t = ingestedAt
	}
	indexName := IndexNameForTime(t)

	doc := logMsg.ToDocument(ingestedAt)
	docBytes, err := json.Marshal(doc)
	if err != nil {
		metrics.ElasticsearchIndexingTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to serialize log document: %w", err)
	}

	res, err := c.es.Index(
		indexName,
		bytes.NewReader(docBytes),
		c.es.Index.WithContext(ctx),
	)
	if err != nil {
		metrics.ElasticsearchIndexingTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		metrics.ElasticsearchIndexingTotal.WithLabelValues("error").Inc()
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("elasticsearch index error on %s: %s (%s)", indexName, res.Status(), string(body))
	}

	metrics.ElasticsearchIndexingTotal.WithLabelValues("success").Inc()
	return nil
}

// SearchLogs executes full-text search and filtering queries on logs-v1-* indices.
func (c *Client) SearchLogs(ctx context.Context, params SearchParams) (*SearchResult, error) {
	page := params.Page
	if page <= 0 {
		page = 1
	}
	size := params.Size
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	fromOffset := (page - 1) * size

	// Build bool query
	boolQuery := make(map[string]any)
	var mustClauses []any
	var filterClauses []any

	if params.Query != "" && params.Query != "*" {
		mustClauses = append(mustClauses, map[string]any{
			"match": map[string]any{
				"message": params.Query,
			},
		})
	} else {
		mustClauses = append(mustClauses, map[string]any{
			"match_all": map[string]any{},
		})
	}

	if params.Service != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{
				"service": params.Service,
			},
		})
	}

	if params.Level != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{
				"level": strings.ToUpper(params.Level),
			},
		})
	}

	if params.TraceID != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{
				"trace_id": params.TraceID,
			},
		})
	}

	if params.From != "" || params.To != "" {
		rangeFilter := make(map[string]any)
		if params.From != "" {
			rangeFilter["gte"] = params.From
		}
		if params.To != "" {
			rangeFilter["lte"] = params.To
		}
		filterClauses = append(filterClauses, map[string]any{
			"range": map[string]any{
				"timestamp": rangeFilter,
			},
		})
	}

	boolQuery["must"] = mustClauses
	if len(filterClauses) > 0 {
		boolQuery["filter"] = filterClauses
	}

	queryBody := map[string]any{
		"query": map[string]any{
			"bool": boolQuery,
		},
		"sort": []any{
			map[string]any{"timestamp": "desc"},
		},
		"from": fromOffset,
		"size": size,
	}

	queryJSON, err := json.Marshal(queryBody)
	if err != nil {
		return nil, fmt.Errorf("failed to encode search query: %w", err)
	}

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(IndexPattern),
		c.es.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to perform search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("elasticsearch search error: %s (%s)", res.Status(), string(body))
	}

	var esResp struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source models.LogDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	total := esResp.Hits.Total.Value
	pages := int(math.Ceil(float64(total) / float64(size)))
	if pages == 0 {
		pages = 1
	}

	logs := make([]*models.LogDocument, 0, len(esResp.Hits.Hits))
	for _, hit := range esResp.Hits.Hits {
		doc := hit.Source
		logs = append(logs, &doc)
	}

	return &SearchResult{
		Total: total,
		Page:  page,
		Size:  size,
		Pages: pages,
		Logs:  logs,
	}, nil
}

// IndexNameForTime generates the daily index name logs-v1-YYYY.MM.DD for a given timestamp.
func IndexNameForTime(t time.Time) string {
	return fmt.Sprintf("logs-v1-%s", t.UTC().Format("2006.01.02"))
}
