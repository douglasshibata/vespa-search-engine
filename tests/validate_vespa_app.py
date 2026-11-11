#!/usr/bin/env python3
"""
Validation Harness for Vespa.ai Application Package, Schemas, and Infra
"""

import os
import sys
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def validate_services_xml():
    services_path = os.path.join(BASE_DIR, "app", "services.xml")
    if not os.path.exists(services_path):
        logging.error(f"services.xml not found at {services_path}")
        return False

    try:
        tree = ET.parse(services_path)
        root = tree.getroot()
        assert root.tag == "services", "Root tag must be <services>"

        container = root.find("container")
        assert container is not None, "Container cluster missing in services.xml"
        assert container.find("document-api") is not None, "<document-api /> missing in container"
        assert container.find("search") is not None, "<search> missing in container"

        content = root.find("content")
        assert content is not None, "Content cluster missing in services.xml"
        assert content.find("documents") is not None, "<documents> missing in content cluster"

        proton = content.find("engine/proton")
        assert proton is not None, "Proton storage engine missing in content cluster"

        admin = root.find("admin")
        assert admin is not None, "Admin plane missing in services.xml"

        logging.info("✅ services.xml validation passed.")
        return True
    except Exception as e:
        logging.error(f"❌ services.xml validation failed: {e}")
        return False

def validate_article_schema():
    schema_path = os.path.join(BASE_DIR, "app", "schemas", "article.sd")
    if not os.path.exists(schema_path):
        logging.error(f"article.sd schema not found at {schema_path}")
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        content = f.read()

    required_tokens = [
        "schema article",
        "document article",
        "field id type string",
        "field title type string",
        "enable-bm25",
        "field embedding type tensor<float>(x[384])",
        "distance-metric: angular",
        "rank-profile default",
        "rank-profile hybrid-multi-phase",
        "first-phase",
        "second-phase",
        "rerank-count: 100"
    ]

    missing = [token for token in required_tokens if token not in content]
    if missing:
        logging.error(f"❌ article.sd schema validation failed. Missing tokens: {missing}")
        return False

    logging.info("✅ article.sd schema validation passed.")
    return True

def validate_item_schema():
    schema_path = os.path.join(BASE_DIR, "app", "schemas", "item.sd")
    if not os.path.exists(schema_path):
        logging.error(f"item.sd schema not found at {schema_path}")
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        content = f.read()

    required_tokens = [
        "schema item",
        "document item",
        "field id type string",
        "field title type string",
        "bolding: on",
        "field embedding type tensor<float>(x[384])",
        "distance-metric: euclidean"
    ]

    missing = [token for token in required_tokens if token not in content]
    if missing:
        logging.error(f"❌ item.sd schema validation failed. Missing tokens: {missing}")
        return False

    logging.info("✅ item.sd schema validation passed.")
    return True

def validate_docker_compose():
    compose_path = os.path.join(BASE_DIR, "docker-compose.yml")
    if not os.path.exists(compose_path):
        logging.error(f"docker-compose.yml not found at {compose_path}")
        return False

    with open(compose_path, "r", encoding="utf-8") as f:
        content = f.read()

    required_services = ["vespa-node", "zookeeper", "kafka", "go-orchestrator", "kafka-feeder"]
    missing = [svc for svc in required_services if svc not in content]
    if missing:
        logging.error(f"❌ docker-compose.yml validation failed. Missing services: {missing}")
        return False

    logging.info("✅ docker-compose.yml validation passed.")
    return True

def main():
    v1 = validate_services_xml()
    v2 = validate_article_schema()
    v3 = validate_item_schema()
    v4 = validate_docker_compose()

    if v1 and v2 and v3 and v4:
        logging.info("🚀 All Vespa workspace validations passed successfully!")
        sys.exit(0)
    else:
        logging.error("💥 Workspace validation failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
