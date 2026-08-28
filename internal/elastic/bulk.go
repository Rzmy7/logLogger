package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
)

// BulkItemError records details of a failed document inside an Elasticsearch _bulk batch.
type BulkItemError struct {
	Index  int    `json:"index"`
	DocID  string `json:"doc_id"`
	Status int    `json:"status"`
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// BulkResult holds the structured outcome of a bulk indexing operation.
type BulkResult struct {
	TotalDocs   int             `json:"total_docs"`
	SuccessDocs int             `json:"success_docs"`
	FailedDocs  int             `json:"failed_docs"`
	Errors      []BulkItemError `json:"errors,omitempty"`
	Duration    time.Duration   `json:"duration"`
	ItemSuccess []bool          `json:"-"`
}

// BulkIndexer defines the contract for micro-batched document indexing into Elasticsearch.
type BulkIndexer interface {
	IndexBatch(ctx context.Context, docs []*models.LogDocument) (*BulkResult, error)
}

type esBulkResponseItem struct {
	Index struct {
		Index  string `json:"_index"`
		ID     string `json:"_id"`
		Status int    `json:"status"`
		Result string `json:"result"`
		Error  *struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	} `json:"index"`
}

type esBulkResponse struct {
	Took   int                  `json:"took"`
	Errors bool                 `json:"errors"`
	Items  []esBulkResponseItem `json:"items"`
}

// IndexBatch executes an Elasticsearch _bulk operation using NDJSON payload.
func (c *Client) IndexBatch(ctx context.Context, docs []*models.LogDocument) (*BulkResult, error) {
	if len(docs) == 0 {
		return &BulkResult{TotalDocs: 0, SuccessDocs: 0, FailedDocs: 0, ItemSuccess: []bool{}}, nil
	}

	start := time.Now()
	now := time.Now().UTC()

	// 1. Build NDJSON bulk payload
	var buf bytes.Buffer
	for _, doc := range docs {
		t, err := doc.ParsedTime()
		if err != nil {
			t = now
		}
		indexName := IndexNameForTime(t)
		docID := doc.DeterministicID()

		// Action header
		meta := fmt.Sprintf(`{"index":{"_index":%q,"_id":%q}}`+"\n", indexName, docID)
		buf.WriteString(meta)

		// Document body
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document for bulk index: %w", err)
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	// 2. Execute Bulk HTTP Request
	res, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
	)
	if err != nil {
		metrics.BulkBatchesTotal.WithLabelValues("failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(len(docs)))
		return nil, fmt.Errorf("elasticsearch bulk request failed: %w", err)
	}
	defer res.Body.Close()

	duration := time.Since(start)
	metrics.BulkBatchDuration.Observe(duration.Seconds())
	metrics.BulkBatchSize.Observe(float64(len(docs)))

	// Check top-level HTTP status
	if res.StatusCode >= 500 || res.StatusCode == http.StatusTooManyRequests {
		metrics.BulkBatchesTotal.WithLabelValues("failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(len(docs)))
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("transient elasticsearch error HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}

	if res.IsError() {
		metrics.BulkBatchesTotal.WithLabelValues("failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(len(docs)))
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("elasticsearch bulk HTTP error %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}

	// 3. Parse JSON response to inspect item-level outcomes
	var bulkResp esBulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		metrics.BulkBatchesTotal.WithLabelValues("failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(len(docs)))
		return nil, fmt.Errorf("failed to decode elasticsearch bulk response: %w", err)
	}

	result := &BulkResult{
		TotalDocs:   len(docs),
		Duration:    duration,
		ItemSuccess: make([]bool, len(docs)),
	}

	// 4. Evaluate individual item statuses
	for i, item := range bulkResp.Items {
		status := item.Index.Status
		isSuccess := status >= 200 && status < 300

		if isSuccess {
			result.SuccessDocs++
			result.ItemSuccess[i] = true
		} else {
			result.FailedDocs++
			result.ItemSuccess[i] = false
			errType := "unknown"
			errReason := "unknown"
			if item.Index.Error != nil {
				errType = item.Index.Error.Type
				errReason = item.Index.Error.Reason
			}
			result.Errors = append(result.Errors, BulkItemError{
				Index:  i,
				DocID:  item.Index.ID,
				Status: status,
				Type:   errType,
				Reason: errReason,
			})
		}
	}

	// 5. Update Prometheus counters
	if result.FailedDocs == 0 {
		metrics.BulkBatchesTotal.WithLabelValues("success").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("success").Add(float64(result.SuccessDocs))
		metrics.ElasticsearchIndexingTotal.WithLabelValues("bulk", "success").Add(float64(result.SuccessDocs))
	} else if result.SuccessDocs > 0 {
		metrics.BulkBatchesTotal.WithLabelValues("partial_failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("success").Add(float64(result.SuccessDocs))
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(result.FailedDocs))
		metrics.ElasticsearchIndexingTotal.WithLabelValues("bulk", "partial").Add(float64(result.TotalDocs))
	} else {
		metrics.BulkBatchesTotal.WithLabelValues("failure").Inc()
		metrics.BulkDocumentsTotal.WithLabelValues("failure").Add(float64(result.FailedDocs))
		metrics.ElasticsearchIndexingTotal.WithLabelValues("bulk", "error").Add(float64(result.FailedDocs))
	}

	return result, nil
}
