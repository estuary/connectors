"""Tests for the socket connector."""

import asyncio
import json
import pytest
from source_socket import (
    SocketEndpointConfig,
    TLSConfig,
    CodecType,
    SocketDocument,
    PlainCodec,
    LineCodec,
    JSONCodec,
    get_codec,
    spec,
    discover,
    validate,
)


class TestConfiguration:
    """Test configuration models."""

    def test_basic_config(self):
        """Test basic endpoint configuration."""
        config = SocketEndpointConfig(
            host="localhost",
            port=8080
        )
        assert config.host == "localhost"
        assert config.port == 8080
        assert config.codec == CodecType.LINE
        assert config.tls.enabled is False

    def test_full_config(self):
        """Test full endpoint configuration."""
        config = SocketEndpointConfig(
            host="secure.example.com",
            port=9443,
            tls=TLSConfig(
                enabled=True,
                verify_cert=True,
                server_hostname="secure.example.com"
            ),
            codec=CodecType.JSON,
            buffer_size=131072,
            reconnect_delay=10.0,
            max_reconnect_attempts=5,
            connection_timeout=60.0,
            idle_timeout=300.0
        )

        assert config.host == "secure.example.com"
        assert config.port == 9443
        assert config.tls.enabled is True
        assert config.codec == CodecType.JSON
        assert config.buffer_size == 131072

    def test_invalid_port(self):
        """Test that invalid port raises error."""
        with pytest.raises(ValueError):
            SocketEndpointConfig(host="localhost", port=0)

        with pytest.raises(ValueError):
            SocketEndpointConfig(host="localhost", port=70000)

    def test_empty_host(self):
        """Test that empty host raises error."""
        with pytest.raises(ValueError):
            SocketEndpointConfig(host="", port=8080)

        with pytest.raises(ValueError):
            SocketEndpointConfig(host="   ", port=8080)


class TestCodecs:
    """Test codec implementations."""

    @pytest.mark.asyncio
    async def test_plain_codec(self):
        """Test plain text codec."""
        codec = PlainCodec()
        data = b"Hello, World!"

        results = [item async for item in codec.decode(data)]
        assert len(results) == 1
        assert results[0] == "Hello, World!"

    @pytest.mark.asyncio
    async def test_line_codec_single_line(self):
        """Test line codec with single line."""
        codec = LineCodec()
        data = b"line one\n"

        results = [item async for item in codec.decode(data)]
        assert len(results) == 1
        assert results[0] == "line one"

    @pytest.mark.asyncio
    async def test_line_codec_multiple_lines(self):
        """Test line codec with multiple lines."""
        codec = LineCodec()
        data = b"line one\nline two\nline three\n"

        results = [item async for item in codec.decode(data)]
        assert len(results) == 3
        assert results[0] == "line one"
        assert results[1] == "line two"
        assert results[2] == "line three"

    @pytest.mark.asyncio
    async def test_line_codec_partial(self):
        """Test line codec with partial data across chunks."""
        codec = LineCodec()

        # First chunk - partial line
        results1 = [item async for item in codec.decode(b"partial ")]
        assert len(results1) == 0

        # Second chunk - completes line
        results2 = [item async for item in codec.decode(b"line\n")]
        assert len(results2) == 1
        assert results2[0] == "partial line"

    @pytest.mark.asyncio
    async def test_line_codec_crlf(self):
        """Test line codec handles CRLF."""
        codec = LineCodec()
        data = b"windows line\r\n"

        results = [item async for item in codec.decode(data)]
        assert len(results) == 1
        assert results[0] == "windows line"

    @pytest.mark.asyncio
    async def test_json_codec_valid(self):
        """Test JSON codec with valid JSON."""
        codec = JSONCodec()
        data = b'{"key": "value", "number": 42}\n'

        results = [item async for item in codec.decode(data)]
        assert len(results) == 1
        assert results[0] == {"key": "value", "number": 42}

    @pytest.mark.asyncio
    async def test_json_codec_multiple(self):
        """Test JSON codec with multiple JSON objects."""
        codec = JSONCodec()
        data = b'{"id": 1}\n{"id": 2}\n{"id": 3}\n'

        results = [item async for item in codec.decode(data)]
        assert len(results) == 3
        assert results[0] == {"id": 1}
        assert results[1] == {"id": 2}
        assert results[2] == {"id": 3}

    @pytest.mark.asyncio
    async def test_json_codec_invalid(self):
        """Test JSON codec with invalid JSON."""
        codec = JSONCodec()
        data = b'not valid json\n'

        results = [item async for item in codec.decode(data)]
        assert len(results) == 1
        assert results[0]["_error"] is True
        assert "_error_message" in results[0]
        assert results[0]["_raw"] == "not valid json"

    def test_get_codec_factory(self):
        """Test codec factory function."""
        assert isinstance(get_codec(CodecType.PLAIN), PlainCodec)
        assert isinstance(get_codec(CodecType.LINE), LineCodec)
        assert isinstance(get_codec(CodecType.JSON), JSONCodec)


class TestDocumentModel:
    """Test document model."""

    def test_socket_document(self):
        """Test socket document creation."""
        doc = SocketDocument(
            source_host="localhost",
            source_port=8080,
            data={"message": "test"},
            sequence=1
        )

        assert doc.source_host == "localhost"
        assert doc.source_port == 8080
        assert doc.data == {"message": "test"}
        assert doc.sequence == 1
        assert doc.timestamp is not None
        assert doc.session_id is not None


class TestConnectorProtocol:
    """Test connector protocol functions."""

    def test_spec(self):
        """Test spec function returns valid schema."""
        result = spec()

        assert "configSchema" in result
        assert "resourceConfigSchema" in result
        assert "documentationUrl" in result

        # Verify schemas are valid JSON Schema
        config_schema = result["configSchema"]
        assert "properties" in config_schema
        assert "host" in config_schema["properties"]
        assert "port" in config_schema["properties"]

    @pytest.mark.asyncio
    async def test_discover(self):
        """Test discover function."""
        config = {
            "host": "example.com",
            "port": 9000,
            "codec": "json"
        }

        result = await discover(config)

        assert "bindings" in result
        assert len(result["bindings"]) == 1

        binding = result["bindings"][0]
        assert "recommendedName" in binding
        assert "resourceConfig" in binding
        assert "documentSchema" in binding
        assert "key" in binding

    @pytest.mark.asyncio
    async def test_validate_invalid_host(self):
        """Test validation with unreachable host."""
        config = {
            "host": "nonexistent.invalid.host.local",
            "port": 9999,
            "connection_timeout": 1.0
        }

        result = await validate(config)

        assert result["valid"] is False
        assert "error" in result


class TestTLSConfig:
    """Test TLS configuration."""

    def test_default_tls_disabled(self):
        """Test TLS is disabled by default."""
        config = TLSConfig()
        assert config.enabled is False
        assert config.verify_cert is True

    def test_tls_enabled(self):
        """Test TLS enabled configuration."""
        config = TLSConfig(
            enabled=True,
            verify_cert=True,
            server_hostname="secure.example.com"
        )

        assert config.enabled is True
        assert config.verify_cert is True
        assert config.server_hostname == "secure.example.com"

    def test_tls_insecure(self):
        """Test insecure TLS configuration."""
        config = TLSConfig(
            enabled=True,
            verify_cert=False
        )

        assert config.enabled is True
        assert config.verify_cert is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])