package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

type clientRegistration struct {
	msgChan chan []byte
	ack     chan struct{}
}

// Broker manages real-time Server-Sent Events (SSE) client subscriptions
type Broker struct {
	Notifier       chan []byte
	newClients     chan clientRegistration
	closingClients chan chan []byte
	clients        map[chan []byte]bool
	mu             sync.Mutex
}

// NewBroker initializes and returns a new SSE Broker instance
func NewBroker() *Broker {
	return &Broker{
		Notifier:       make(chan []byte, 100),
		newClients:     make(chan clientRegistration),
		closingClients: make(chan chan []byte),
		clients:        make(map[chan []byte]bool),
	}
}

// Start runs the event loop listening for new clients, disconnects, and broadcast messages
func (b *Broker) Start() {
	go func() {
		for {
			select {
			case reg := <-b.newClients:
				b.mu.Lock()
				b.clients[reg.msgChan] = true
				b.mu.Unlock()
				close(reg.ack)
				log.Printf("SSE Client connected. Total clients: %d", len(b.clients))

			case s := <-b.closingClients:
				b.mu.Lock()
				delete(b.clients, s)
				close(s)
				b.mu.Unlock()
				log.Printf("SSE Client disconnected. Total clients: %d", len(b.clients))

			case event := <-b.Notifier:
				b.mu.Lock()
				for clientMessageChan := range b.clients {
					select {
					case clientMessageChan <- event:
					default:
						log.Println("SSE Client message buffer full, skipping event")
					}
				}
				b.mu.Unlock()
			}
		}
	}()
}

// ServeHTTP handles incoming HTTP GET connections for SSE streaming (/events)
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Flush response headers immediately
	flusher.Flush()

	messageChan := make(chan []byte, 10)
	ack := make(chan struct{})
	b.newClients <- clientRegistration{msgChan: messageChan, ack: ack}
	<-ack

	defer func() {
		b.closingClients <- messageChan
	}()

	notify := r.Context().Done()

	for {
		select {
		case <-notify:
			return
		case msg, ok := <-messageChan:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
