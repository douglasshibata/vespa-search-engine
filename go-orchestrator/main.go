package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	vespaURL := os.Getenv("VESPA_URL")
	if vespaURL == "" {
		vespaURL = "http://localhost:8080"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8090"
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "vespa-item-events"
	}

	// 1. Initialize Real-Time SSE Broker
	sseBroker := NewBroker()
	sseBroker.Start()

	// 2. Launch Kafka-to-Vespa Indexer background worker if Kafka broker is configured
	if kafkaBroker != "" {
		log.Printf("Starting Kafka Vespa Indexer worker (Kafka: %s, Topic: %s)...", kafkaBroker, kafkaTopic)
		go StartVespaIndexer(kafkaBroker, kafkaTopic, sseBroker)
	}

	client := NewVespaClient(vespaURL)

	// Health Check
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"UP","engine":"Vespa Golang Orchestrator"}`))
	})

	// Server-Sent Events Endpoint for real-time notifications
	http.Handle("/events", sseBroker)
	http.Handle("/api/v1/events", sseBroker)

	// Articles REST Endpoint
	http.HandleFunc("/api/v1/articles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// Ingestion handler
			var doc ArticleFields
			doc.ID = fmt.Sprintf("doc-%d", time.Now().UnixNano())
			doc.Title = "Enterprise Multi-Phase Search with Vespa Engine"
			doc.Body = "Vespa provides real-time HNSW vector search and hybrid two-phase ranking."
			doc.Category = "technology"
			doc.Popularity = 9.5
			doc.Timestamp = time.Now().Unix()

			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()

			if err := client.PutDocument(ctx, doc); err != nil {
				http.Error(w, fmt.Sprintf("Vespa PUT Error: %v", err), http.StatusInternalServerError)
				return
			}

			// Broadcast event to connected SSE clients
			sseBroker.Notifier <- []byte(fmt.Sprintf(`{"status":"indexed","id":"%s","type":"article"}`, doc.ID))

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(fmt.Sprintf(`{"message":"Document indexed successfully","id":"%s"}`, doc.ID)))
			return
		}

		if r.Method == http.MethodGet {
			// Search query handler
			query := r.URL.Query().Get("q")
			category := r.URL.Query().Get("category")

			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()

			searchReq := SearchRequest{
				Query:       query,
				Category:    category,
				RankProfile: "hybrid-multi-phase",
				Hits:        10,
			}

			result, err := client.Search(ctx, searchReq)
			if err != nil {
				http.Error(w, fmt.Sprintf("Vespa Search Error: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"hits_count":%d}`, len(result.Root.Children))
			return
		}

		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	})

	log.Printf("🚀 Vespa Golang Orchestrator listening on :%s (Vespa Endpoint: %s)...", port, vespaURL)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
