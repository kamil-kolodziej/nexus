"""Unit tests for HealthEndpoint."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_ingestion.monitoring.health_endpoint import HealthEndpoint


class TestHealthEndpointStop:
    @pytest.mark.asyncio
    async def test_stop_awaits_task_cancellation(self) -> None:
        """stop() must await the cancelled task so uvicorn does not linger."""
        endpoint = HealthEndpoint(port=9999)

        async def slow_serve() -> None:
            try:
                await asyncio.sleep(9999)
            except asyncio.CancelledError:
                raise

        with patch("uvicorn.Server") as mock_server_cls:
            mock_server = MagicMock()
            mock_server.serve = slow_serve
            mock_server_cls.return_value = mock_server
            await endpoint.start()

        task = endpoint._task
        assert task is not None

        # Simulate timeout: wait_for raises, then cancel + gather must clean up
        async def raising_wait_for(coro_or_task, timeout):  # type: ignore[no-untyped-def]
            raise asyncio.TimeoutError()

        with patch("nexus_ingestion.monitoring.health_endpoint.asyncio.wait_for", side_effect=raising_wait_for):
            await endpoint.stop()

        # The task must be done after stop() fully awaits the cancellation
        assert task.done()

    @pytest.mark.asyncio
    async def test_stop_is_idempotent_when_no_server(self) -> None:
        """stop() must not raise if start() was never called."""
        endpoint = HealthEndpoint(port=9999)
        await endpoint.stop()  # must not raise
