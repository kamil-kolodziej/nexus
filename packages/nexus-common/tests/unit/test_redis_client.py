"""Unit tests for the Redis client factory."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from nexus_common.redis_client import _sanitize_url, create_redis_client


class TestSanitizeUrl:
    def test_strips_credentials_from_url(self) -> None:
        url = "redis://:mysecret@localhost:6379"
        result = _sanitize_url(url)
        assert "mysecret" not in result
        assert "localhost" in result

    def test_url_with_username_and_password(self) -> None:
        url = "redis://user:pass@redis.example.com:6379/0"  # pragma: allowlist secret
        result = _sanitize_url(url)
        assert "pass" not in result
        assert "user" not in result
        assert "redis.example.com" in result

    def test_url_without_credentials_unchanged(self) -> None:
        result = _sanitize_url("redis://localhost:6379")
        assert "localhost" in result
        assert "6379" in result

    def test_url_without_port(self) -> None:
        result = _sanitize_url("redis://localhost")
        assert "localhost" in result
        # Should not raise


class TestCreateRedisClient:
    @pytest.mark.asyncio
    async def test_returns_redis_client_on_success(self) -> None:
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(return_value=True)

        with patch("nexus_common.redis_client.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = mock_client
            client = await create_redis_client("redis://localhost:6379")

        assert client is mock_client
        mock_client.ping.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ping_failure_propagates(self) -> None:
        mock_client = AsyncMock()
        mock_client.ping.side_effect = ConnectionError("Redis unreachable")

        with patch("nexus_common.redis_client.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = mock_client
            with pytest.raises(ConnectionError):
                await create_redis_client("redis://localhost:6379")

    @pytest.mark.asyncio
    async def test_passes_decode_responses_and_retry_on_timeout(self) -> None:
        mock_client = AsyncMock()

        with patch("nexus_common.redis_client.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = mock_client
            await create_redis_client(decode_responses=False, retry_on_timeout=False)

        call_kwargs = mock_redis_cls.from_url.call_args[1]
        assert call_kwargs["decode_responses"] is False
        assert call_kwargs["retry_on_timeout"] is False

    @pytest.mark.asyncio
    async def test_default_url_is_localhost(self) -> None:
        mock_client = AsyncMock()

        with patch("nexus_common.redis_client.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = mock_client
            await create_redis_client()

        call_args = mock_redis_cls.from_url.call_args[0]
        assert "localhost" in call_args[0]
