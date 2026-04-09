"""Entry point for the nexus-sentiment service."""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog
from nexus_common.logging import configure_logging
from nexus_common.redis_client import create_redis_client

from nexus_sentiment.config import SentimentConfig
from nexus_sentiment.monitoring.health_endpoint import HealthEndpoint
from nexus_sentiment.persistence.timescale_writer import TimescaleWriter
from nexus_sentiment.publishers.health_publisher import HealthPublisher
from nexus_sentiment.publishers.redis_publisher import RedisPublisher
from nexus_sentiment.service import SentimentService

logger = structlog.get_logger()


def _request_shutdown(stop_event: asyncio.Event) -> None:
    """Signal handler — sets the stop event to trigger graceful shutdown."""
    logger.info("shutdown_requested")
    stop_event.set()


def _create_processor(processor_type: str):
    """Factory: create processor based on config."""
    if processor_type == "vader":
        from nexus_sentiment.processors.vader_processor import VaderProcessor

        return VaderProcessor()
    elif processor_type == "finbert":
        try:
            from nexus_sentiment.processors.finbert_processor import FinBertProcessor
        except ImportError:
            logger.error(
                "finbert_dependencies_missing",
                message="Install nexus-sentiment[finbert] for FinBERT support "
                "(requires transformers and torch).",
            )
            sys.exit(1)
        return FinBertProcessor()
    else:
        logger.error("unknown_processor_type", processor_type=processor_type)
        sys.exit(1)


async def run() -> None:
    """Main async entry point."""
    config = SentimentConfig()
    configure_logging(env=config.log_env)

    # Create processor and load model
    processor = _create_processor(config.processor_type)
    try:
        await processor.load()
    except Exception:
        logger.error("model_load_failed", processor_type=config.processor_type, exc_info=True)
        sys.exit(1)

    # Create Redis connection
    redis = await create_redis_client(config.redis_url)

    # Create publishers
    redis_publisher = RedisPublisher(
        redis,
        config.output_stream,
        maxlen=config.output_maxlen,
    )
    health_publisher = HealthPublisher(
        redis,
        config.health_stream,
        maxlen=config.health_maxlen,
    )

    # Create TimescaleDB writer (best-effort)
    timescale_writer = None
    try:
        timescale_writer = TimescaleWriter(
            config.timescaledb_dsn,
            batch_size=config.batch_size,
            flush_interval=config.flush_interval,
            health_callback=health_publisher.publish,
        )
    except Exception:
        logger.warning("timescale_writer_init_failed", exc_info=True)

    # Create health endpoint
    health_endpoint = HealthEndpoint(port=config.health_port, host=config.health_host)

    # Create asset extractor if dictionary exists
    asset_extractor = None
    try:
        from nexus_sentiment.extraction.asset_extractor import AssetExtractor

        asset_extractor = AssetExtractor(
            dictionary_path=config.asset_dictionary_path,
            active_assets=set(config.active_assets),
        )
    except FileNotFoundError:
        logger.error(
            "asset_dictionary_missing",
            path=config.asset_dictionary_path,
        )
        sys.exit(1)
    except Exception:
        logger.error("asset_extractor_init_failed", exc_info=True)
        sys.exit(1)

    # Wire service
    service = SentimentService(
        config=config,
        redis=redis,
        processor=processor,
        redis_publisher=redis_publisher,
        health_publisher=health_publisher,
        timescale_writer=timescale_writer,
        health_endpoint=health_endpoint,
        asset_extractor=asset_extractor,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, stop_event)

    await service.start()
    await stop_event.wait()
    await service.stop()


def main() -> None:
    """Synchronous entry point."""
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop_installed")
    except ImportError:
        logger.info("uvloop_unavailable")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
