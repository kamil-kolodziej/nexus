"""Unit tests for the sentiment health endpoint (RFC draft-inadarei format)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from nexus_sentiment.monitoring.health_endpoint import (
    HEALTH_MEDIA_TYPE,
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
