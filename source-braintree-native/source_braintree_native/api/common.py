import asyncio
from braintree import (
    BraintreeGateway,
    AddOnGateway,
    Configuration,
    )
from braintree.attribute_getter import AttributeGetter
from datetime import datetime, timedelta, UTC
from typing import Awaitable, Any, AsyncGenerator
import xmltodict
import re

CONVENIENCE_OBJECTS = [
    'gateway'
]

HEADERS = {
    "X-ApiVersion": "6",
    "Accept": "application/xml",
}

# Searches return at most 10,000 results (50,000 for transaction searches). If we hit this limit,
# the connector could have missed data and we'll need to use smaller date windows.
SEARCH_LIMIT = 10_000
TRANSACTION_SEARCH_LIMIT = 50_000

SEARCH_PAGE_SIZE = 50
SEMAPHORE_LIMIT = 20


async def process_completed_fetches(
    fetch_coroutines: list[Awaitable[list[dict[str, Any]]]],
) -> AsyncGenerator[dict[str, Any], None]:
    """Helper to process fetching multiple pages of resources and yield individual resources."""
    for coro in asyncio.as_completed(fetch_coroutines):
        result = await coro
        for resource in result:
            yield resource


def braintree_xml_to_dict(xml_data):
    def parse_node(obj):
        if isinstance(obj, dict):
            # Handle array-typed elements first - if it has @type="array", return the array content
            if obj.get('@type') == 'array':
                # Find the first non-@ key which contains the actual array items
                for inner_key, inner_value in obj.items():
                    if not inner_key.startswith('@'):
                        # Convert the inner array content
                        converted_inner = parse_node(inner_value)
                        # Ensure it's a list (single items might not be in a list) 
                        # but preserve the full structure of each item
                        if not isinstance(converted_inner, list):
                            converted_inner = [converted_inner] if converted_inner is not None else []
                        return converted_inner
                # No content found, return empty list
                return []

            # Check if this is a simple text node (has only text content + attributes)
            non_attr_keys = [k for k in obj.keys() if not k.startswith('@')]

            # Case 1: No child elements at all - empty element
            if len(non_attr_keys) == 0:
                # No content at all - return node_content with None  
                return node_content(obj, None)
            # Case 2: Only has #text key - this is simple text content
            elif len(non_attr_keys) == 1 and '#text' in obj:
                # Only text content - return node_content with text value
                return node_content(obj, obj['#text'])
            # Case 3: Has child elements - build dict (even if just one child)
            # This matches Braintree SDK behavior: if there are child elements, build dict

            # Case 2: Complex object with child elements - build dict
            new_dict = {}
            for key, value in obj.items():
                # Skip XML metadata attributes and text content
                if key.startswith('@') or key == '#text':
                    continue

                # Convert key format: "search-results" -> "search_results"  
                new_key = key.replace('-', '_')
                parsed_value = parse_node(value)

                # Handle multiple elements with same name (convert to list)
                if new_key in new_dict:
                    if not isinstance(new_dict[new_key], list):
                        new_dict[new_key] = [new_dict[new_key]]
                    new_dict[new_key].append(parsed_value)
                else:
                    new_dict[new_key] = parsed_value
            return new_dict

        elif isinstance(obj, list):
            return [parse_node(item) for item in obj]
        elif obj is None:
            # Handle None values (empty elements from xmltodict)
            return ""
        else:
            # Simple string/number value
            return obj

    def node_content(parent, content):
        parent_type = parent.get('@type') if isinstance(parent, dict) else None
        parent_nil = parent.get('@nil') if isinstance(parent, dict) else None

        if parent_nil == 'true':
            return None
        elif parent_type == 'integer':
            return int(content) if content is not None and content != "" else 0
        elif parent_type == 'boolean':
            if content == "true" or content == "1":
                return True
            else:
                return False
        elif parent_type == 'datetime':
            if content and content.strip():
                # Parse datetime to match Braintree SDK behavior
                # Remove timezone info and microseconds to match Braintree SDK
                clean_content = re.sub(r'[TZ]', ' ', content).strip()
                clean_content = re.sub(r'\.\d+', '', clean_content)  # Remove microseconds
                return datetime.strptime(clean_content, '%Y-%m-%d %H:%M:%S')
            return content or ""
        elif parent_type == 'date':
            if content and content.strip():
                # Parse date to match Braintree SDK behavior  
                return datetime.strptime(content, '%Y-%m-%d').date()
            return content or ""
        elif parent_type == 'decimal':
            return content or ""  # Return as string like Braintree SDK, empty string if None
        else:
            return content or ""

    parsed = xmltodict.parse(xml_data)
    return parse_node(parsed)


def braintree_object_to_dict(braintree_object):
    """
    Recursively convert a Braintree object and its nested objects to a dictionary.
    Memory-optimized version that minimizes temporary objects.
    """
    if isinstance(braintree_object, (Configuration, AddOnGateway, BraintreeGateway)):
        return None

    source_data = braintree_object.__dict__
    data = {}

    for key, value in source_data.items():
        # Skip convenience and private attributes upfront
        if key in CONVENIENCE_OBJECTS or key == '_setattrs':
            continue

        if isinstance(value, AttributeGetter):
            data[key] = braintree_object_to_dict(value)
        elif isinstance(value, datetime):
            data[key] = value.replace(tzinfo=UTC)
        elif hasattr(value, "__dict__"):
            data[key] = braintree_object_to_dict(value)
        elif isinstance(value, list):
            data[key] = [
                braintree_object_to_dict(item) if hasattr(item, "__dict__")
                else item.replace(tzinfo=UTC) if isinstance(item, datetime)
                else item
                for item in value
            ]
        else:
            data[key] = value

    return data


def reduce_window_end(
    start: datetime,
    end: datetime,
) -> datetime:
    window_size = (end - start) / 2

    # Braintree's datetimes have a resolution of seconds, so we remove microseconds from the window size.
    reduced_window_size = window_size - timedelta(microseconds=window_size.microseconds)

    # It's unlikely a user will have enough data in Braintree that the connector will reduce the window size below 1 second,
    # but if it does happen the connector should raise an error since that stream will be stuck.
    if reduced_window_size < timedelta(seconds=1):
        raise RuntimeError("Window size is smaller than Braintree's datetime resolution of 1 second. Contact Estuary support for help addressing this error.")

    return start + reduced_window_size


def search_limit_error_message(count: int) -> str:
    msg = (
        f"{count} returned in a single search which is "
        f"greater than or equal to Braintree's documented maximum for a single search. "
        "Reduce the window size and backfill this stream."
    )

    return msg
