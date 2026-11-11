package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildYQL(t *testing.T) {
	t.Run("Text query only", func(t *testing.T) {
		yql := BuildYQL("golang vespa", "", false)
		expected := `select * from article where userQuery() or title contains "golang vespa" or body contains "golang vespa"`
		if yql != expected {
			t.Errorf("Expected YQL: %s, got: %s", expected, yql)
		}
	})

	t.Run("Category filter and embedding", func(t *testing.T) {
		yql := BuildYQL("", "tech", true)
		if !strings.Contains(yql, `category contains "tech"`) {
			t.Errorf("Expected category filter in YQL, got: %s", yql)
		}
		if !strings.Contains(yql, `nearestNeighbor(embedding, q_embedding)`) {
			t.Errorf("Expected nearestNeighbor in YQL, got: %s", yql)
		}
	})

	t.Run("Special character escaping", func(t *testing.T) {
		yql := BuildYQL(`hello "world"`, `cat"1`, false)
		if strings.Contains(yql, `"world"`) {
			t.Errorf("Double quotes should be escaped in YQL, got: %s", yql)
		}
	})
}

func TestVespaClient_PutDocument(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got: %s", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/document/v1/article/article/docid/") {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}

		var payload VespaPutPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("Failed to decode body: %v", err)
		}

		if payload.Fields.Title != "Test Title" {
			t.Errorf("Expected title 'Test Title', got: %s", payload.Fields.Title)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"pathId":"/document/v1/article/article/docid/test-123","id":"id:article:article::test-123"}`))
	}))
	defer server.Close()

	client := NewVespaClient(server.URL)

	doc := ArticleFields{
		ID:         "test-123",
		Title:      "Test Title",
		Body:       "Test Body",
		Category:   "general",
		Popularity: 5.0,
		Timestamp:  123456789,
	}

	err := client.PutDocument(context.Background(), doc)
	if err != nil {
		t.Fatalf("PutDocument failed: %v", err)
	}
}

func TestVespaClient_Search(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got: %s", r.Method)
		}

		queryYQL := r.URL.Query().Get("yql")
		if queryYQL == "" {
			t.Error("Expected non-empty YQL query parameter")
		}

		ranking := r.URL.Query().Get("ranking")
		if ranking != "hybrid-multi-phase" {
			t.Errorf("Expected ranking 'hybrid-multi-phase', got: %s", ranking)
		}

		resp := VespaQueryResult{}
		resp.Root.Children = []VespaHit{
			{
				ID:        "id:article:article::1",
				Relevance: 0.95,
				Fields: ArticleFields{
					ID:    "1",
					Title: "Result 1",
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewVespaClient(server.URL)

	req := SearchRequest{
		Query:       "test",
		Category:    "tech",
		Embedding:   []float32{0.1, 0.2, 0.3},
		RankProfile: "hybrid-multi-phase",
		Hits:        5,
	}

	res, err := client.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(res.Root.Children) != 1 {
		t.Fatalf("Expected 1 hit, got %d", len(res.Root.Children))
	}

	if res.Root.Children[0].Relevance != 0.95 {
		t.Errorf("Expected relevance 0.95, got %f", res.Root.Children[0].Relevance)
	}
}
