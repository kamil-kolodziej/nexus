"""Unit tests for the sentiment health endpoint (RFC draft-inadarei format)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from nexus_sentiment.monitoring.health_endpoint import (
    HEALTH_MEDIA_TYPE,
    HealthEndpoint,
    create_health_app,
)


def _ok_body():
    return {
        "status": "ok",
        "serviceId": "nexus-sentiment",
        "version": "0.1.0",
        "checks": {
            "processor:inference": {"status": "ok", "observedValue": "vader:3.3.2"},
            "redis:publisher": {"status": "ok", "observedValue": 0},
        },
    }


def _degraded_body():
    body = _ok_body()
    body["status"] = "degraded"
    body["checks"]["redis:publisher"] = {
        "status": "degraded",
        "output": "disconnected, buffering 3 events",
        "observedValue": 3,
    }
    return body


def _error_body():
    body = _ok_body()
    body["status"] = "error"
    body["checks"]["processor:inference"] = {
        "status": "error",
        "output": "5 consecutive inference failures",
        "observedValue": 5,
    }
    return body


class TestHealthEndpoint:
    @pytest.mark.parametrize(
        "body_factory,expected_code",
        [(_ok_body, 200), (_degraded_body, 200), (_error_body, 503)],
    )
    async def test_status_code_matches_status(self, body_factory, expected_code):
        app = create_health_app()
        app.state.health_provider = body_factory
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == expected_code
        assert resp.headers["content-type"].startswith(HEALTH_MEDIA_TYPE)

    async def test_rfc_shape(self):
        app = create_health_app()
        app.state.health_provider = _ok_body
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        data = resp.json()
        assert data["status"] == "ok"
        assert data["serviceId"] == "nexus-sentiment"
        assert "version" in data
        assert "processor:inference" in data["checks"]
        assert data["checks"]["processor:inference"]["status"] == "ok"

    async def test_default_body_when_no_provider(self):
        app = create_health_app()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["serviceId"] == "nexus-sentiment"
        assert data["checks"] == {}


class TestHealthEndpointLifecycle:
    """Drive HealthEndpoint.start()/stop() with a stubbed uvicorn.Server."""

    @pytest.fixture
    def patched_server_factory(self, monkeypatch):
        """Replace uvicorn.Server with a controllable async stub."""
        from nexus_sentiment.monitoring import health_endpoint as he_module

        servers: list = []

        class FakeServer:
            def __init__(self, _config):
                self._config = _config
                self.should_exit = False
                self.serve_started = asyncio.Event()
                self.serve_finished = asyncio.Event()
                servers.append(self)

            async def serve(self):
                self.serve_started.set()
                # Block until should_exit is flipped (mirrors real uvicorn loop).
                while not self.should_exit:
                    await asyncio.sleep(0.01)
                self.serve_finished.set()

        monkeypatch.setattr(he_module.uvicorn, "Server", FakeServer)
        return servers

    async def test_start_spawns_task_and_sets_provider(self, patched_server_factory):
        ep = HealthEndpoint(port=0)
        provider = lambda: {"status": "ok", "serviceId": "x", "checks": {}}  # noqa: E731
        ep.set_health_provider(provider)
        assert ep.app.state.health_provider is provider

        await ep.start()
        try:
            assert ep._task is not None
            assert ep._server is patched_server_factory[0]
            # Wait for serve() to actually begin so we exercise the loop.
            await asyncio.wait_for(ep._server.serve_started.wait(), timeout=1)
        finally:
            await ep.stop()

    async def test_stop_signals_shutdown_and_awaits_task(self, patched_server_factory):
        ep = HealthEndpoint(port=0)
        await ep.start()
        await asyncio.wait_for(ep._server.serve_started.wait(), timeout=1)

        await ep.stop()

        assert ep._server.should_exit is True
        assert ep._task is not None
        assert ep._task.done()
        assert ep._server.serve_finished.is_set()

    async def test_stop_when_task_hangs_cancels_after_timeout(self, monkeypatch):
        from nexus_sentiment.monitoring import health_endpoint as he_module

        class HangingServer:
            def __init__(self, _config):
                self.should_exit = False

            async def serve(self):
                # Ignore should_exit so wait_for must time out and cancel.
                await asyncio.sleep(60)

        monkeypatch.setattr(he_module.uvicorn, "Server", HangingServer)

        ep = HealthEndpoint(port=0)
        await ep.start()
        # Avoid waiting the real 5s timeout — patch wait_for to short-circuit.
        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout):
            return await original_wait_for(coro, timeout=0.05)

        monkeypatch.setattr(he_module.asyncio, "wait_for", fast_wait_for)

        await ep.stop()

        assert ep._task is not None
        assert ep._task.done()

    async def test_on_task_done_silent_on_cancel(self):
        ep = HealthEndpoint(port=0)
        task = MagicMock()
        task.cancelled.return_value = True
        # Should not raise.
        ep._on_task_done(task)

    async def test_on_task_done_logs_on_exception(self):
        ep = HealthEndpoint(port=0)
        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = RuntimeError("boom")
        # Should not raise.
        ep._on_task_done(task)

    async def test_app_property_returns_fastapi(self):
        ep = HealthEndpoint(port=0)
        from fastapi import FastAPI

        assert isinstance(ep.app, FastAPI)


# Silence unused-import warning when AsyncMock is not used elsewhere in this file.
_ = AsyncMock
