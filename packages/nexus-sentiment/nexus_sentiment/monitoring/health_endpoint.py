"""FastAPI health endpoint for the sentiment service."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

logger = structlog.get_logger()


class SentimentHealth(BaseModel):
    """Runtime status of the sentiment service."""

    status: str  # "ok", "degraded", "error"
    processor_type: str
    processor_state: str  # "loaded", "failed"
    model_id: str
    events_processed: int
    errors: int


def create_health_app() -> FastAPI:
    """Create the FastAPI app with health endpoint."""
    app = FastAPI(title="Nexus Sentiment Health", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        if hasattr(app.state, "health_provider"):
            return app.state.health_provider().model_dump(mode="json")
        return SentimentHealth(
            status="ok",
            processor_type="unknown",
            processor_state="unknown",
            model_id="",
            events_processed=0,
            errors=0,
        ).model_dump(mode="json")

    return app


class HealthEndpoint:
    """Runs the FastAPI health endpoint as an asyncio task."""

    def __init__(self, port: int = 8081, host: str = "127.0.0.1") -> None:
        self._port = port
        self._host = host
        self._app = create_health_app()
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def app(self) -> FastAPI:
        return self._app

    def set_health_provider(self, provider: Any) -> None:
        """Set the callable that returns SentimentHealth."""
        self._app.state.health_provider = provider

    async def start(self) -> None:
        """Start the uvicorn server as an asyncio task."""
        config = uvicorn.Config(
            self._app,
            host=self._host,
            port=self._port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve(), name="health-endpoint")
        logger.info("health_endpoint_started", port=self._port)

    async def stop(self) -> None:
        """Shutdown the server."""
        if self._server:
            self._server.should_exit = True
        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
