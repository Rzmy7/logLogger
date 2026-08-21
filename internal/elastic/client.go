package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

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

// Indexer defines the contract for indexing documents into Elasticsearch.
type Indexer interface {
	EnsureTemplate(ctx context.Context) error
	IndexLog(ctx context.Context, logMsg *models.LogMessage, ingestedAt time.Time) error
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
		Addresses: []string{esURL},
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

// EnsureTemplate creates or updates the logs-v1-template index template.
func (c *Client) EnsureTemplate(ctx context.Context) error {
	req := strings.NewReader(IndexTemplateDefinition)
	res, err := c.es.Indices.PutIndexTemplate(
		TemplateName,
		req,
		c.es.Indices.PutIndexTemplate.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to create index template: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("error creating index template %s: %s (%s)", TemplateName, res.Status(), string(body))
	}

	log.Printf("[INFO] Elasticsearch index template %q verified/created successfully", TemplateName)
	return nil
}

// IndexLog indexes a single log document into the appropriate daily index (logs-v1-YYYY.MM.DD).
func (c *Client) IndexLog(ctx context.Context, logMsg *models.LogMessage, ingestedAt time.Time) error {
	if logMsg == nil {
		return errors.New("log message cannot be nil")
	}

	t, err := logMsg.ParsedTime()
	if err != nil {
		t = ingestedAt
	}
	indexName := IndexNameForTime(t)

	doc := logMsg.ToDocument(ingestedAt)
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to serialize log document: %w", err)
	}

	res, err := c.es.Index(
		indexName,
		bytes.NewReader(docBytes),
		c.es.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("elasticsearch index error on %s: %s (%s)", indexName, res.Status(), string(body))
	}

	return nil
}

// IndexNameForTime generates the daily index name logs-v1-YYYY.MM.DD for a given timestamp.
func IndexNameForTime(t time.Time) string {
	return fmt.Sprintf("logs-v1-%s", t.UTC().Format("2006.01.02"))
}
