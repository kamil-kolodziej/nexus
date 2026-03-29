"""Entry point for the nexus-ingestion service."""

from __future__ import annotations

import asyncio
import logging
import signal

from nexus_common.redis_client import create_redis_client

from nexus_ingestion.adapters.exchange_adapter import ExchangeAdapter
from nexus_ingestion.adapters.news_adapter import NewsAdapter
from nexus_ingestion.config import IngestionConfig
from nexus_ingestion.monitoring.gap_detector import GapDetector
from nexus_ingestion.monitoring.health_endpoint import HealthEndpoint
from nexus_ingestion.persistence.timescale_writer import TimescaleWriter
from nexus_ingestion.publishers.health_publisher import HealthPublisher
from nexus_ingestion.publishers.redis_publisher import RedisPublisher
from nexus_ingestion.service import IngestionService

logger = logging.getLogger(__name__)


def _request_shutdown(stop_event: asyncio.Event) -> None:
    """Signal handler — sets the stop event to trigger graceful shutdown."""
    logger.info("Received shutdown signal, stopping...")
    stop_event.set()


async def run() -> None:
    """Main async entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = IngestionConfig()
    service = IngestionService(config)

    # Create Redis connection
    redis = await create_redis_client(config.redis_url)

    # Create publishers
    market_publisher = RedisPublisher(
        redis,
        config.market_events_stream,
        maxlen=config.market_events_maxlen,
        buffer_max=config.redis_buffer_max,
    )
    news_publisher = RedisPublisher(
        redis,
        config.news_events_stream,
        maxlen=config.news_events_maxlen,
        buffer_max=config.redis_buffer_max,
    )
    health_publisher = HealthPublisher(
        redis,
        config.health_events_stream,
        maxlen=config.health_events_maxlen,
    )
    service.set_publishers(redis, market_publisher, health_publisher, news_publisher)

    # Create TimescaleDB writer
    writer = TimescaleWriter(
        config.timescaledb_dsn,
        batch_size=config.batch_size,
        flush_interval=config.flush_interval,
        queue_maxsize=config.queue_maxsize,
        health_callback=service.handle_health_alert,
    )
    service.set_timescale_writer(writer)

    # Create gap detector
    gap_detector = GapDetector(
        gap_threshold=config.gap_threshold,
        malformed_rate_threshold=config.malformed_rate_threshold,
        health_callback=service.handle_health_alert,
    )
    service.set_gap_detector(gap_detector)

    # Create health endpoint
    health_endpoint = HealthEndpoint(port=config.health_port, host=config.health_host)
    service.set_health_endpoint(health_endpoint)

    # Create and register exchange adapter
    exchange_adapter = ExchangeAdapter(
        config.exchange_id,
        api_key=config.exchange_api_key,
        api_secret=config.exchange_api_secret,
        sandbox=config.exchange_sandbox,
        assets=config.subscribed_assets,
        timestamp_tolerance=config.timestamp_tolerance,
        event_callback=service.handle_event,
        health_callback=service.handle_health_alert,
    )
    service.register_adapter(exchange_adapter)

    # Register news adapters
    for news_source in config.news_sources:
        if news_source.name and news_source.url:
            news_adapter = NewsAdapter(
                source_name=news_source.name,
                source_url=news_source.url,
                source_type=news_source.type,
                poll_interval=config.news_poll_interval,
                event_callback=service.handle_event,
                health_callback=service.handle_health_alert,
            )
            service.register_adapter(news_adapter)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, stop_event)

    await service.start()
    await stop_event.wait()
    await service.stop()


def main() -> None:
    """Synchronous entry point — sets up uvloop and runs the async main."""
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop installed as event loop policy")
    except ImportError:
        logger.info("uvloop not available, using default event loop")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
