"""Integration tests for the health endpoint.

Verifies GET /health returns per-adapter status with correct fields and <200ms response.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from nexus_common.schemas.enums import AdapterStatus
from nexus_common.schemas.health_alert import AdapterHealth

from nexus_ingestion.monitoring.health_endpoint import create_health_app


@pytest.fixture
def app():
    app = create_health_app()
    app.state.adapter_healths = lambda: [
        AdapterHealth(
            adapter_id="binance:exchange",
            adapter_type="exchange",
            status=AdapterStatus.CONNECTED,
            event_count=42,
            error_count=0,
            malformed_count=0,
        ),
    ]
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_ok_status(self, client) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_health_returns_adapter_list(self, client) -> None:
        resp = client.get("/health")
        data = resp.json()
        assert len(data["adapters"]) == 1
        adapter = data["adapters"][0]
        assert adapter["adapter_id"] == "binance:exchange"
        assert adapter["adapter_type"] == "exchange"
        assert adapter["status"] == "CONNECTED"
        assert adapter["event_count"] == 42

    def test_health_response_under_200ms(self, client) -> None:
        start = time.monotonic()
        resp = client.get("/health")
        elapsed_ms = (time.monotonic() - start) * 1000
        assert resp.status_code == 200
        assert elapsed_ms < 200

    def test_health_degraded_when_no_connected(self) -> None:
        app = create_health_app()
        app.state.adapter_healths = lambda: [
            AdapterHealth(
                adapter_id="binance:exchange",
                adapter_type="exchange",
                status=AdapterStatus.DOWN,
            ),
        ]
        client = TestClient(app)
        resp = client.get("/health")
        data = resp.json()
        assert data["status"] == "degraded"

    def test_health_ok_when_no_adapters(self) -> None:
        app = create_health_app()
        app.state.adapter_healths = lambda: []
        client = TestClient(app)
        resp = client.get("/health")
        data = resp.json()
        assert data["status"] == "ok"
