package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProcessKafkaMessage(t *testing.T) {
	var receivedBody string
	var receivedPath string

	vespaMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("Expected PUT method, got: %s", r.Method)
		}
		receivedPath = r.URL.Path
		bodyBytes, _ := io.ReadAll(r.Body)
		receivedBody = string(bodyBytes)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"pathId":"/document/v1/namespace/item/docid/item-1","id":"id:namespace:item::item-1"}`))
	}))
	defer vespaMock.Close()

	broker := NewBroker()

	key := []byte("item-1")
	value := []byte("Wireless Headphones")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ProcessKafkaMessage(ctx, key, value, vespaMock.URL, vespaMock.Client(), broker)
	if err != nil {
		t.Fatalf("ProcessKafkaMessage failed: %v", err)
	}

	if !strings.Contains(receivedPath, "/document/v1/namespace/item/docid/item-1") {
		t.Errorf("Unexpected path received by Vespa mock: %s", receivedPath)
	}

	if !strings.Contains(receivedBody, `"title": "Wireless Headphones"`) {
		t.Errorf("Unexpected body payload received: %s", receivedBody)
	}

	// Verify notification channel
	select {
	case notification := <-broker.Notifier:
		if !strings.Contains(string(notification), "item-1") {
			t.Errorf("Expected notification for item-1, got: %s", string(notification))
		}
	default:
		t.Error("Expected SSE notification in broker, but none received")
	}
}
