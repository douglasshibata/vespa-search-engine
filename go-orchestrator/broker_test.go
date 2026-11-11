package main

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBroker_SSEBroadcasting(t *testing.T) {
	broker := NewBroker()
	broker.Start()

	server := httptest.NewServer(broker)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect to SSE broker: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("Expected Content-Type text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Broadcast message via broker
	testPayload := []byte(`{"status":"indexed","id":"item-123"}`)
	broker.Notifier <- testPayload

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read SSE line: %v", err)
	}

	if !strings.HasPrefix(line, "data: ") {
		t.Errorf("Expected SSE data line, got: %s", line)
	}

	if !strings.Contains(line, "item-123") {
		t.Errorf("Expected payload in SSE stream, got: %s", line)
	}
}
