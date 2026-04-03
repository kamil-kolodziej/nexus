"""Unit tests for the sentiment health endpoint."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from nexus_sentiment.monitoring.health_endpoint import (
    SentimentHealth,
    create_health_app,
)


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    @pytest.fixture
    def app(self):
        app = create_health_app()
        app.state.health_provider = lambda: SentimentHealth(
            status="ok",
            processor_type="vader",
            processor_state="loaded",
            model_id="vader:3.3.2",
            events_processed=42,
            errors=1,
        )
        return app

    async def test_health_returns_json(self, app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["processor_type"] == "vader"
        assert data["processor_state"] == "loaded"
        assert data["model_id"] == "vader:3.3.2"
        assert data["events_processed"] == 42
        assert data["errors"] == 1

    async def test_health_without_provider(self):
        app = create_health_app()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["events_processed"] == 0
