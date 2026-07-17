"""
Azure Service Bus drain logic, ported from standalone/push_servicebus.py.

Each invocation opens an async receiver, drains available messages in batches,
emits one document per message, and — crucially — completes (settles) each batch
ONLY after Estuary has durably checkpointed its documents. That ordering makes
the connector at-least-once: a crash between checkpoint and settle redelivers the
messages, and the /sequence_number key upserts the duplicates away.
"""

import json
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusReceiveMode

from estuary_cdk.capture.common import LogCursor

from .models import EndpointConfig, ServiceBusLog


def _decode(v: Any) -> Any:
    return v.decode("utf-8", "replace") if isinstance(v, bytes) else v


def _to_document(msg: Any) -> ServiceBusLog:
    try:
        body: Any = json.loads(str(msg))
    except (json.JSONDecodeError, UnicodeDecodeError):
        body = {"raw": str(msg)}

    enqueued = msg.enqueued_time_utc
    return ServiceBusLog(
        sequence_number=int(msg.sequence_number),
        message_id=msg.message_id,
        enqueued_time=enqueued.isoformat() if enqueued else None,
        subject=msg.subject,
        content_type=msg.content_type,
        correlation_id=msg.correlation_id,
        application_properties={
            _decode(k): _decode(v)
            for k, v in (msg.application_properties or {}).items()
        },
        body=body,
    )


def _make_receiver(client: ServiceBusClient, config: EndpointConfig):
    if config.queue_name:
        return client.get_queue_receiver(
            queue_name=config.queue_name,
            receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
            prefetch_count=config.advanced.prefetch_count,
        )
    # Guaranteed by EndpointConfig's validator (queue XOR topic+subscription).
    assert config.topic_name and config.subscription_name
    return client.get_subscription_receiver(
        topic_name=config.topic_name,
        subscription_name=config.subscription_name,
        receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
        prefetch_count=config.advanced.prefetch_count,
    )


async def fetch_changes(
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ServiceBusLog | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last = log_cursor

    def _next_cursor() -> datetime:
        nonlocal last
        last = max(datetime.now(tz=UTC), last + timedelta(milliseconds=1))
        return last

    entity = config.entity_name()
    max_total = config.advanced.max_messages_per_sweep
    batch_size = config.advanced.prefetch_count
    wait = config.advanced.max_wait_seconds

    log.info(f"[{entity}] draining (max {max_total} msgs, {wait}s idle timeout)")

    drained = 0
    client = ServiceBusClient.from_connection_string(config.connection_string)
    async with client:
        receiver = _make_receiver(client, config)
        async with receiver:
            while drained < max_total:
                remaining = max_total - drained
                batch = await receiver.receive_messages(
                    max_message_count=min(batch_size, remaining),
                    max_wait_time=wait,
                )
                if not batch:
                    log.info(f"[{entity}] entity drained after {drained} messages")
                    break

                for msg in batch:
                    yield _to_document(msg)

                # Checkpoint the batch's documents. Code after this yield only runs
                # once the runtime has awaited task.checkpoint(), so completing here
                # is safe (docs are durable before the messages leave the broker).
                yield _next_cursor()

                for msg in batch:
                    await receiver.complete_message(msg)

                drained += len(batch)
                log.info(f"[{entity}] batch settled — {drained} messages so far")

    # Final checkpoint (also covers the "nothing available" case).
    yield _next_cursor()
