#!/usr/bin/env python3
"""
Kafka to Vespa Document Feeder
Consumes source change events from Apache Kafka and streams them asynchronously
to the Vespa Document API to keep the search index synchronized.
"""

import os
import json
import time
import urllib.request
import urllib.error
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

VESPA_URL = os.environ.get("VESPA_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "vespa-article-events")

def transform_to_vespa_document(event: dict) -> dict:
    """
    Transforms a raw database event into Vespa Document API format.
    """
    doc_id = event.get("id", f"article-{int(time.time()*1000)}")
    return {
        "id": doc_id,
        "fields": {
            "id": doc_id,
            "title": event.get("title", ""),
            "body": event.get("body", ""),
            "category": event.get("category", "general"),
            "popularity": float(event.get("popularity", 1.0)),
            "timestamp": int(event.get("timestamp", time.time())),
            "embedding": {
                "values": event.get("embedding", [0.0] * 384)
            }
        }
    }

def post_to_vespa(vespa_url: str, document: dict) -> bool:
    """
    Posts document payload to Vespa HTTP REST Document API.
    """
    doc_id = document["id"]
    endpoint = f"{vespa_url.rstrip('/')}/document/v1/article/article/docid/{doc_id}"

    payload = json.dumps({"fields": document["fields"]}).encode('utf-8')
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status in (200, 201):
                logging.info(f"Successfully fed document {doc_id} to Vespa.")
                return True
            else:
                logging.warning(f"Failed to feed document {doc_id}. Status: {response.status}")
                return False
    except urllib.error.HTTPError as e:
        logging.error(f"HTTPError feeding {doc_id}: {e.code} - {e.read().decode('utf-8', errors='ignore')}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error feeding {doc_id}: {e}")
        return False

def simulate_stream_feeder():
    """
    Simulates Kafka message stream consumer for offline/demonstration testing.
    """
    logging.info(f"Starting Kafka-Vespa Sync Feeder (Target Vespa: {VESPA_URL}, Topic: {TOPIC})...")

    mock_events = [
        {
            "id": "kafka-doc-101",
            "title": "Real-Time Big Data Processing with Vespa and Kafka",
            "body": "Stream processing pipelines feed high-throughput document events directly into Proton storage nodes.",
            "category": "distributed-systems",
            "popularity": 9.8,
            "embedding": [0.01 * (i % 10) for i in range(384)]
        },
        {
            "id": "kafka-doc-102",
            "title": "HNSW Vector Search in Low Latency Environments",
            "body": "Vespa provides integrated ANN vector indexing using HNSW graph algorithms combined with BM25 text retrieval.",
            "category": "search-ai",
            "popularity": 8.7,
            "embedding": [0.02 * (i % 5) for i in range(384)]
        }
    ]

    for event in mock_events:
        doc = transform_to_vespa_document(event)
        post_to_vespa(VESPA_URL, doc)
        time.sleep(1)

if __name__ == "__main__":
    simulate_stream_feeder()
