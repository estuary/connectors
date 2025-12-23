#!/usr/bin/env python3

"""
High-performance fixture generator for Flow collections.
Generates millions of documents in seconds using efficient streaming and batching.
"""

import json
import os
import sys
import uuid
import random
from typing import Any

# Configuration
PERF_DOC_COUNT = int(os.environ.get('PERF_DOC_COUNT', '100000'))
BATCH_SIZE = 10000  # Write in batches for better I/O performance

def generate_uuid() -> str:
    """Generate a random UUID string."""
    return str(uuid.uuid4())

def generate_value(field_name: str, field_type: str, index: int) -> Any:
    """Generate realistic data based on field name and type."""
    field_name_lower = field_name.lower()

    if field_type in ('integer', 'number'):
        # Sequential for ID/key fields, random for others
        if any(pattern in field_name_lower for pattern in ('id', 'key')):
            return index
        else:
            return random.randint(0, 999999)

    elif field_type == 'string':
        # Type-specific strings
        if any(pattern in field_name_lower for pattern in ('id', 'uuid', 'key')):
            return generate_uuid()
        elif 'name' in field_name_lower:
            return f"name_{index}_{generate_uuid()[:8]}"
        elif 'email' in field_name_lower:
            return f"user{index}@example.com"
        else:
            return f"value_{index}_{generate_uuid()[:8]}"

    else:
        return None

def generate_document(collection: str, index: int, schema: dict, properties: list[str]) -> dict:
    """Generate a single document based on schema."""
    doc = {}

    for prop in properties:
        prop_schema = schema['properties'].get(prop, {})
        prop_type = prop_schema.get('type', 'string')

        # Handle array types (take first type)
        if isinstance(prop_type, list):
            prop_type = prop_type[0] if prop_type else 'string'

        doc[prop] = generate_value(prop, prop_type, index)

    return doc

def extract_perf_collections(resources_config: str) -> set[str]:
    """Extract collections matching 'tests/perf-*' pattern."""
    try:
        resources = json.loads(resources_config)
        return {
            r['source']
            for r in resources
            if 'source' in r and 'tests/perf-' in r['source']
        }
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing RESOURCES_CONFIG: {e}", file=sys.stderr)
        return set()

def load_flow_config(flow_json_path: str) -> dict:
    """Load and parse flow.json configuration."""
    try:
        with open(flow_json_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading flow.json: {e}", file=sys.stderr)
        sys.exit(1)

def write_phase_documents(f, collection: str, schema: dict, properties: list[str],
                         start_idx: int, count: int) -> None:
    """
    Write documents for a phase, one per line.
    """
    batch = []

    for i in range(start_idx, start_idx + count):
        doc = generate_document(collection, i, schema, properties)
        entry = [collection, doc]
        batch.append(entry)

        # Write batch when it's full
        if len(batch) >= BATCH_SIZE:
            for entry in batch:
                f.write(json.dumps(entry, separators=(',', ':')) + '\n')
            batch = []

        # Progress indicator
        if (i - start_idx) > 0 and (i - start_idx) % 10000 == 0:
            print(f"  Generated {i - start_idx}/{count} documents for {collection}...", file=sys.stderr)

    # Write remaining documents
    for entry in batch:
        f.write(json.dumps(entry, separators=(',', ':')) + '\n')

def write_ack(f):
    """Write an ack marker to indicate end of transaction."""
    f.write('{"ack":true}\n')

def main():
    if len(sys.argv) != 3:
        print("Usage: generate-fixture.py <flow.json> <output-fixture.json>", file=sys.stderr)
        sys.exit(1)

    flow_json_path = sys.argv[1]
    output_fixture_path = sys.argv[2]

    # Load configuration
    resources_config = os.environ.get('RESOURCES_CONFIG', '[]')
    perf_collections = extract_perf_collections(resources_config)

    if not perf_collections:
        print("No performance collections found (pattern: tests/perf-*)", file=sys.stderr)
        with open(output_fixture_path, 'w') as f:
            f.write('[]')
        return

    flow_config = load_flow_config(flow_json_path)
    collections_config = flow_config.get('collections', {})

    print(f"Generating {PERF_DOC_COUNT} documents for {len(perf_collections)} collection(s)...", file=sys.stderr)

    # Prepare collection schemas
    collection_info = {}
    for collection in sorted(perf_collections):
        if collection not in collections_config:
            print(f"Warning: Collection {collection} not found in flow.json", file=sys.stderr)
            continue

        schema = collections_config[collection].get('schema')
        if not schema:
            print(f"Warning: No schema found for collection {collection}", file=sys.stderr)
            continue

        properties = list(schema.get('properties', {}).keys())
        collection_info[collection] = {
            'schema': schema,
            'properties': properties
        }

    if not collection_info:
        print("Error: No valid collections found", file=sys.stderr)
        sys.exit(1)

    # Generate fixture with streaming writes
    update_count = PERF_DOC_COUNT // 10

    with open(output_fixture_path, 'w', buffering=8192*16) as f:  # 128KB buffer
        # Phase 0: Empty (just ack)
        write_ack(f)

        # Phase 1: Initial bulk load
        for collection in sorted(collection_info.keys()):
            info = collection_info[collection]
            print(f"Generating {PERF_DOC_COUNT} documents for {collection}...", file=sys.stderr)
            write_phase_documents(
                f, collection, info['schema'], info['properties'],
                0, PERF_DOC_COUNT
            )
        write_ack(f)

        # Phase 2: Update batch (10% of documents)
        for collection in sorted(collection_info.keys()):
            info = collection_info[collection]
            print(f"Generating {update_count} update documents for {collection}...", file=sys.stderr)
            write_phase_documents(
                f, collection, info['schema'], info['properties'],
                0, update_count
            )
        write_ack(f)

    print(f"Generated fixture with {PERF_DOC_COUNT} documents per collection", file=sys.stderr)
    print(f"Output: {output_fixture_path}", file=sys.stderr)

if __name__ == '__main__':
    main()
