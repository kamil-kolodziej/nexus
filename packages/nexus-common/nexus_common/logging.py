"""Structured logging configuration for Nexus services.

Usage in a service entry point::

    from nexus_common.logging import configure_logging
    configure_logging(env="development")   # human-readable
    configure_logging()                    # JSON (default: production)

Usage in any module::

    import structlog
    logger = structlog.get_logger()

    # Module-level (no bound context)
    logger.info("redis_connected", url=url)

    # Class-level (bind once, reuse)
    self._logger = structlog.get_logger().bind(adapter_id=self.adapter_id)
    self._logger.warning("publish_failed", buffer_size=len(self._buffer))
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(*, env: str = "production", level: int = logging.INFO) -> None:
    """Configure structlog + stdlib to emit structured log records.

    Args:
        env: ``"development"`` for human-readable console output;
             any other value produces JSON (suitable for log aggregators).
        level: Minimum log level applied to the root stdlib logger.
    """
    # Processors shared by structlog-originated records and foreign (stdlib) records.
    # They run *before* the final renderer on every log line.
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if env == "development":
        final_renderer: Any = structlog.dev.ConsoleRenderer()
    else:
        final_renderer = structlog.processors.JSONRenderer()

    # Wire structlog to hand off records to stdlib's ProcessorFormatter.
    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # The ProcessorFormatter finishes formatting (exception rendering + final renderer)
    # for both structlog records and foreign records (uvicorn, asyncpg, ccxt …).
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.ExceptionRenderer(),
            final_renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
