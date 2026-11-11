package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProcessKafkaMessage transforms a Kafka message, sends an HTTP PUT to Vespa Document API, and broadcasts via SSE Broker
func ProcessKafkaMessage(ctx context.Context, key, value []byte, vespaBaseURL string, httpClient *http.Client, broker *Broker) error {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	if vespaBaseURL == "" {
		vespaBaseURL = "http://localhost:8080"
	}
	vespaBaseURL = strings.TrimRight(vespaBaseURL, "/")

	docID := string(key)
	if docID == "" {
		docID = fmt.Sprintf("item-%d", time.Now().UnixNano())
	}

	// 1. Transform Kafka message to Vespa JSON Feed format
	vespaDocumentURL := fmt.Sprintf("%s/document/v1/namespace/item/docid/%s", vespaBaseURL, docID)

	// Payload matching Vespa schema
	vespaPayload := []byte(fmt.Sprintf(`{"fields": {"title": "%s"}}`, string(value)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, vespaDocumentURL, bytes.NewBuffer(vespaPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// 2. Send to Vespa
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to put document to Vespa: %w", err)
	}
	defer resp.Body.Close()

	// 3. Broadcast real-time update to Next.js clients via SSE Broker
	if broker != nil && broker.Notifier != nil {
		notification := fmt.Sprintf(`{"status": "indexed", "id": "%s"}`, docID)
		broker.Notifier <- []byte(notification)
	}

	return nil
}

// StartVespaIndexer consumes Kafka events and indexes them into Vespa, broadcasting via SSE Broker
func StartVespaIndexer(brokerAddress, topic string, broker *Broker) {
	vespaURL := os.Getenv("VESPA_URL")
	if vespaURL == "" {
		vespaURL = "http://localhost:8080"
	}
	StartVespaIndexerWithContext(context.Background(), brokerAddress, topic, vespaURL, broker)
}

// StartVespaIndexerWithContext consumes Kafka events with context control for graceful shutdown
func StartVespaIndexerWithContext(ctx context.Context, brokerAddress, topic, vespaBaseURL string, broker *Broker) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "vespa-index-worker",
	})
	defer r.Close()

	client := &http.Client{Timeout: 10 * time.Second}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			_ = ProcessKafkaMessage(ctx, m.Key, m.Value, vespaBaseURL, client, broker)
		}
	}
}
