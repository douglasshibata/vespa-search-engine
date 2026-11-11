package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ArticleFields defines the document fields stored in Vespa
type ArticleFields struct {
	ID         string                 `json:"id"`
	Title      string                 `json:"title"`
	Body       string                 `json:"body"`
	Category   string                 `json:"category"`
	Embedding  map[string]interface{} `json:"embedding,omitempty"`
	Popularity float64                `json:"popularity"`
	Timestamp  int64                  `json:"timestamp"`
}

// VespaPutPayload represents the HTTP PUT body format expected by Vespa Document API
type VespaPutPayload struct {
	Fields ArticleFields `json:"fields"`
}

// SearchRequest holds parameters for querying Vespa
type SearchRequest struct {
	Query     string    `json:"query"`
	Category  string    `json:"category"`
	Embedding []float32 `json:"embedding,omitempty"`
	RankProfile string  `json:"rankProfile"`
	Hits      int       `json:"hits"`
}

// VespaHit represents a single result hit from Vespa
type VespaHit struct {
	ID        string        `json:"id"`
	Relevance float64       `json:"relevance"`
	Fields    ArticleFields `json:"fields"`
}

// VespaQueryResult represents the root JSON response from Vespa Search API
type VespaQueryResult struct {
	Root struct {
		ID        string     `json:"id"`
		Relevance float64    `json:"relevance"`
		Children  []VespaHit `json:"children"`
	} `json:"root"`
}

// VespaClient manages HTTP communication with Vespa Container Cluster
type VespaClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewVespaClient initializes a client pointing to the Vespa container endpoint
func NewVespaClient(baseURL string) *VespaClient {
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	return &VespaClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// BuildYQL converts structured search input into Vespa Query Language (YQL)
func BuildYQL(textQuery string, category string, hasEmbedding bool) string {
	conditions := []string{}

	if textQuery != "" {
		// Escape double quotes to prevent YQL injection
		cleanText := strings.ReplaceAll(textQuery, `"`, `\"`)
		conditions = append(conditions, fmt.Sprintf(`userQuery() or title contains "%s" or body contains "%s"`, cleanText, cleanText))
	}

	if category != "" {
		cleanCat := strings.ReplaceAll(category, `"`, `\"`)
		conditions = append(conditions, fmt.Sprintf(`category contains "%s"`, cleanCat))
	}

	if hasEmbedding {
		conditions = append(conditions, `({targetHits: 100}nearestNeighbor(embedding, q_embedding))`)
	}

	whereClause := "true"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	return fmt.Sprintf("select * from article where %s", whereClause)
}

// PutDocument streams a document into Vespa Content Cluster via Document API
func (c *VespaClient) PutDocument(ctx context.Context, doc ArticleFields) error {
	documentURI := fmt.Sprintf("%s/document/v1/article/article/docid/%s", c.BaseURL, url.PathEscape(doc.ID))

	payload := VespaPutPayload{
		Fields: doc,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, documentURI, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("http execution failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vespa document put failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// Search executes a search query against Vespa Container API using YQL and ranking profiles
func (c *VespaClient) Search(ctx context.Context, sr SearchRequest) (*VespaQueryResult, error) {
	searchURL := fmt.Sprintf("%s/search/", c.BaseURL)

	yql := BuildYQL(sr.Query, sr.Category, len(sr.Embedding) > 0)

	params := url.Values{}
	params.Set("yql", yql)

	if sr.Query != "" {
		params.Set("query", sr.Query)
	}

	if sr.RankProfile != "" {
		params.Set("ranking", sr.RankProfile)
	} else {
		params.Set("ranking", "hybrid-multi-phase")
	}

	if sr.Hits > 0 {
		params.Set("hits", fmt.Sprintf("%d", sr.Hits))
	} else {
		params.Set("hits", "10")
	}

	// Format vector embedding parameter if present
	if len(sr.Embedding) > 0 {
		embedJSON, err := json.Marshal(map[string]interface{}{
			"values": sr.Embedding,
		})
		if err == nil {
			params.Set("input.query(q_embedding)", string(embedJSON))
		}
	}

	reqURL := fmt.Sprintf("%s?%s", searchURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create search request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("search http execution failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vespa search failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result VespaQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	return &result, nil
}
