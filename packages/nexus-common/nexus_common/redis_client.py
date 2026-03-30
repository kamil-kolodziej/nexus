"""Async Redis connection factory with retry support."""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

import structlog
from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

logger = structlog.get_logger()


def _sanitize_url(url: str) -> str:
    """Strip credentials from a Redis URL for safe logging."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunparse(parsed._replace(netloc=netloc))


async def create_redis_client(
    url: str = "redis://localhost:6379",
    *,
    decode_responses: bool = True,
    retry_on_timeout: bool = True,
    max_retries: int = 3,
) -> Redis[str]:
    """Create an async Redis client with exponential backoff retry."""
    retry = Retry(ExponentialBackoff(), retries=max_retries)
    client: Redis[str] = Redis.from_url(  # type: ignore[call-overload]
        url,
        decode_responses=decode_responses,
        retry_on_timeout=retry_on_timeout,
        retry=retry,
    )
    # Verify connectivity
    await client.ping()
    logger.info("redis_connected", url=_sanitize_url(url))
    return client
