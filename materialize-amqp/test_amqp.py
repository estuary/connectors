"""Tests for the AMQP connector."""

import json
import pytest
from datetime import datetime, timezone
from pydantic import SecretStr

from source_amqp import (
    AMQPEndpointConfig,
    TLSConfig,
    QueueConfig,
    ExchangeBindingConfig,
    ConsumerConfig,
    AMQPDocument,
    AMQPMessageProperties,
    AMQPDeliveryInfo,
    AuthType,
    CaptureState,
    spec,
    discover,
)


class TestConfiguration:
    """Test configuration models."""

    def test_minimal_config(self):
        """Test minimal endpoint configuration."""
        config = AMQPEndpointConfig(
            queue=QueueConfig(name="test_queue")
        )

        assert config.host == "localhost"
        assert config.port == 5672
        assert config.virtual_host == "/"
        assert config.username == "guest"
        assert config.queue.name == "test_queue"
        assert config.tls.enabled is False

    def test_full_config(self):
        """Test full endpoint configuration."""
        config = AMQPEndpointConfig(
            host="rabbitmq.example.com",
            port=5671,
            virtual_host="/production",
            auth_type=AuthType.PLAIN,
            username="myuser",
            password=SecretStr("mypass"),
            tls=TLSConfig(
                enabled=True,
                verify_cert=True,
                server_hostname="rabbitmq.example.com"
            ),
            queue=QueueConfig(
                name="events",
                durable=True,
                exclusive=False,
                auto_delete=False,
                arguments={"x-max-length": 10000}
            ),
            bindings=[
                ExchangeBindingConfig(
                    exchange="orders",
                    routing_key="orders.#"
                )
            ],
            consumer=ConsumerConfig(
                prefetch_count=200,
                no_ack=False,
                priority=5
            ),
            heartbeat=30,
            connection_timeout=60.0,
            max_priority=10,
            message_ttl=3600000
        )

        assert config.host == "rabbitmq.example.com"
        assert config.port == 5671
        assert config.tls.enabled is True
        assert config.queue.name == "events"
        assert config.queue.durable is True
        assert len(config.bindings) == 1
        assert config.bindings[0].exchange == "orders"
        assert config.consumer.prefetch_count == 200
        assert config.max_priority == 10

    def test_amqp_url_generation(self):
        """Test AMQP URL generation."""
        config = AMQPEndpointConfig(
            host="broker.example.com",
            port=5672,
            virtual_host="/myapp",
            username="user1",
            password=SecretStr("secret123"),
            queue=QueueConfig(name="test")
        )

        url = config.get_amqp_url()
        assert url == "amqp://user1:secret123@broker.example.com:5672//myapp"

    def test_amqp_url_with_tls(self):
        """Test AMQP URL with TLS enabled."""
        config = AMQPEndpointConfig(
            host="secure.example.com",
            port=5671,
            tls=TLSConfig(enabled=True),
            queue=QueueConfig(name="test")
        )

        url = config.get_amqp_url()
        assert url.startswith("amqps://")


class TestQueueConfig:
    """Test queue configuration."""

    def test_basic_queue(self):
        """Test basic queue configuration."""
        queue = QueueConfig(name="my_queue")

        assert queue.name == "my_queue"
        assert queue.durable is True
        assert queue.exclusive is False
        assert queue.auto_delete is False

    def test_queue_with_arguments(self):
        """Test queue with x-arguments."""
        queue = QueueConfig(
            name="priority_queue",
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-message-ttl": 86400000,
                "x-dead-letter-exchange": "dlx"
            }
        )

        assert queue.arguments["x-max-priority"] == 10
        assert "x-dead-letter-exchange" in queue.arguments


class TestExchangeBinding:
    """Test exchange binding configuration."""

    def test_direct_binding(self):
        """Test direct exchange binding."""
        binding = ExchangeBindingConfig(
            exchange="direct_exchange",
            routing_key="my.routing.key"
        )

        assert binding.exchange == "direct_exchange"
        assert binding.routing_key == "my.routing.key"

    def test_topic_binding_wildcard(self):
        """Test topic exchange with wildcard."""
        binding = ExchangeBindingConfig(
            exchange="topic_exchange",
            routing_key="orders.*.us"
        )

        assert binding.routing_key == "orders.*.us"

    def test_headers_binding(self):
        """Test headers exchange binding."""
        binding = ExchangeBindingConfig(
            exchange="headers_exchange",
            routing_key="",
            arguments={
                "x-match": "all",
                "region": "us",
                "type": "order"
            }
        )

        assert binding.arguments["x-match"] == "all"


class TestConsumerConfig:
    """Test consumer configuration."""

    def test_default_consumer(self):
        """Test default consumer settings."""
        consumer = ConsumerConfig()

        assert consumer.prefetch_count == 100
        assert consumer.no_ack is False
        assert consumer.exclusive is False
        assert consumer.priority is None

    def test_high_throughput_consumer(self):
        """Test high throughput consumer."""
        consumer = ConsumerConfig(
            prefetch_count=500,
            no_ack=False,
            priority=10
        )

        assert consumer.prefetch_count == 500
        assert consumer.priority == 10


class TestTLSConfig:
    """Test TLS configuration."""

    def test_default_disabled(self):
        """Test TLS disabled by default."""
        tls = TLSConfig()
        assert tls.enabled is False
        assert tls.verify_cert is True

    def test_tls_enabled(self):
        """Test TLS enabled."""
        tls = TLSConfig(
            enabled=True,
            verify_cert=True,
            server_hostname="broker.example.com"
        )

        assert tls.enabled is True
        assert tls.server_hostname == "broker.example.com"


class TestDocumentModels:
    """Test document models."""

    def test_message_properties(self):
        """Test AMQP message properties."""
        props = AMQPMessageProperties(
            content_type="application/json",
            priority=5,
            correlation_id="corr-123",
            headers={"x-custom": "value"}
        )

        assert props.content_type == "application/json"
        assert props.priority == 5
        assert props.headers["x-custom"] == "value"

    def test_delivery_info(self):
        """Test delivery info."""
        delivery = AMQPDeliveryInfo(
            consumer_tag="ctag-1",
            delivery_tag=42,
            redelivered=False,
            exchange="my_exchange",
            routing_key="my.key"
        )

        assert delivery.delivery_tag == 42
        assert delivery.routing_key == "my.key"

    def test_amqp_document(self):
        """Test full AMQP document."""
        doc = AMQPDocument(
            body={"order_id": "123", "amount": 99.99},
            body_size=50,
            properties=AMQPMessageProperties(
                content_type="application/json",
                priority=3
            ),
            delivery=AMQPDeliveryInfo(
                consumer_tag="ctag-1",
                delivery_tag=100,
                exchange="orders",
                routing_key="orders.created"
            ),
            queue="order_events",
            virtual_host="/prod",
            sequence=1
        )

        assert doc.body["order_id"] == "123"
        assert doc.properties.priority == 3
        assert doc.delivery.routing_key == "orders.created"
        assert doc.queue == "order_events"


class TestCaptureState:
    """Test capture state."""

    def test_default_state(self):
        """Test default capture state."""
        state = CaptureState()

        assert state.last_delivery_tag == 0
        assert state.total_messages == 0
        assert state.reconnect_count == 0

    def test_state_with_data(self):
        """Test state with data."""
        state = CaptureState(
            last_delivery_tag=500,
            total_messages=10000,
            total_bytes=1024000,
            last_routing_key="orders.created.us"
        )

        assert state.last_delivery_tag == 500
        assert state.total_messages == 10000


class TestConnectorProtocol:
    """Test connector protocol functions."""

    def test_spec(self):
        """Test spec returns valid schema."""
        result = spec()

        assert "configSchema" in result
        assert "resourceConfigSchema" in result
        assert "documentationUrl" in result

        config_schema = result["configSchema"]
        assert "properties" in config_schema
        assert "host" in config_schema["properties"]
        assert "queue" in config_schema["properties"]

    @pytest.mark.asyncio
    async def test_discover(self):
        """Test discover function."""
        config = {
            "host": "localhost",
            "queue": {
                "name": "test_events"
            }
        }

        result = await discover(config)

        assert "bindings" in result
        assert len(result["bindings"]) == 1

        binding = result["bindings"][0]
        assert "recommendedName" in binding
        assert "documentSchema" in binding
        assert "key" in binding


if __name__ == "__main__":
    pytest.main([__file__, "-v"])