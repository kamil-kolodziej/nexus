"""Integration tests for the health endpoint.

Verifies GET /health returns RFC draft-inadarei-api-health-check format
with correct status and <200ms response.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient
from nexus_ingestion.monitoring.health_endpoint import HEALTH_MEDIA_TYPE, create_health_app


def _ok_body():
    return {
        "status": "ok",
        "serviceId": "nexus-ingestion",
        "version": "0.1.0",
        "checks": {
            "adapter:connections": {"status": "ok", "observedValue": 1},
            "redis:publisher": {"status": "ok", "observedValue": 0},
        },
    }


def _degraded_body():
    body = _ok_body()
    body["status"] = "degraded"
    body["checks"]["adapter:connections"] = {
        "status": "degraded",
        "output": "1 of 1 adapters unhealthy",
        "observedValue": 1,
    }
    return body


@pytest.fixture
def app():
    app = create_health_app()
    app.state.health_provider = _ok_body
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200_with_ok_status(self, client) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith(HEALTH_MEDIA_TYPE)
        data = resp.json()
        assert data["status"] == "ok"

    def test_health_rfc_shape(self, client) -> None:
        resp = client.get("/health")
        data = resp.json()
        assert "status" in data
        assert "serviceId" in data
        assert "version" in data
        assert "checks" in data
        assert "adapter:connections" in data["checks"]
        assert "redis:publisher" in data["checks"]

    def test_health_response_under_200ms(self, client) -> None:
        start = time.monotonic()
        resp = client.get("/health")
        elapsed_ms = (time.monotonic() - start) * 1000
        assert resp.status_code == 200
        assert elapsed_ms < 200

    def test_health_returns_degraded_when_adapter_down(self) -> None:
        app = create_health_app()
        app.state.health_provider = _degraded_body
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"

    def test_health_returns_503_when_status_error(self) -> None:
        app = create_health_app()
        app.state.health_provider = lambda: {
            "status": "error",
            "serviceId": "nexus-ingestion",
            "version": "0.1.0",
            "checks": {
                "adapter:connections": {
                    "status": "error",
                    "output": "all adapters disconnected",
                    "observedValue": 2,
                }
            },
        }
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 503

    def test_health_default_body_when_no_provider(self) -> None:
        app = create_health_app()
        client = TestClient(app)
        resp = client.get("/health")
        data = resp.json()
        assert data["status"] == "ok"
        assert data["serviceId"] == "nexus-ingestion"
        assert data["checks"] == {}
