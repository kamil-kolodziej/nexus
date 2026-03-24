"""Async Redis connection factory with retry support."""

from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

logger = logging.getLogger(__name__)


async def create_redis_client(
    url: str = "redis://localhost:6379",
    *,
    decode_responses: bool = True,
    retry_on_timeout: bool = True,
    max_retries: int = 3,
) -> Redis:
    """Create an async Redis client with exponential backoff retry."""
    retry = Retry(ExponentialBackoff(), retries=max_retries)
    client = Redis.from_url(
        url,
        decode_responses=decode_responses,
        retry_on_timeout=retry_on_timeout,
        retry=retry,
    )
    # Verify connectivity
    await client.ping()
    logger.info("Redis connection established: %s", url)
    return client
