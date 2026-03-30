"""FastAPI health endpoint for ingestion service."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI

logger = structlog.get_logger()


def create_health_app() -> FastAPI:
    """Create the FastAPI app with health endpoint."""
    app = FastAPI(title="Nexus Ingestion Health", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        healths = app.state.adapter_healths() if hasattr(app.state, "adapter_healths") else []
        adapters = [h.model_dump(mode="json") for h in healths]

        # Overall status: ok if any adapter is connected, degraded otherwise
        if not adapters:
            status = "ok"
        elif any(a["status"] == "CONNECTED" for a in adapters):
            status = "ok"
        else:
            status = "degraded"

        return {"status": status, "adapters": adapters}

    return app


class HealthEndpoint:
    """Runs the FastAPI health endpoint as an asyncio task."""

    def __init__(self, port: int = 8080, host: str = "127.0.0.1") -> None:
        self._port = port
        self._host = host
        self._app = create_health_app()
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def app(self) -> FastAPI:
        return self._app

    def set_adapter_healths_provider(self, provider: Any) -> None:
        """Set the callable that returns list of AdapterHealth."""
        self._app.state.adapter_healths = provider

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
