"""FastAPI health endpoint for the sentiment service.

Implements the RFC draft-inadarei-api-health-check response format.
See docs/design/nexus-trading-platform-design.md § Monitoring.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

logger = structlog.get_logger()

HEALTH_MEDIA_TYPE = "application/health+json"


def _status_code(status: str) -> int:
    return 503 if status == "error" else 200


def create_health_app() -> FastAPI:
    """Create the FastAPI app with health endpoint."""
    app = FastAPI(title="Nexus Sentiment Health", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> JSONResponse:
        if hasattr(app.state, "health_provider"):
            body: dict[str, Any] = app.state.health_provider()
        else:
            body = {
                "status": "ok",
                "serviceId": "nexus-sentiment",
                "checks": {},
            }
        return JSONResponse(
            content=body,
            status_code=_status_code(body.get("status", "ok")),
            media_type=HEALTH_MEDIA_TYPE,
        )

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
        """Set the callable that returns the RFC-shaped health body as a dict."""
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
        self._task.add_done_callback(self._on_task_done)
        logger.info("health_endpoint_started", port=self._port)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        """FR-009 supervision: log if the health endpoint task crashes."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("health_endpoint_task_crashed", error=str(exc), exc_info=exc)

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
