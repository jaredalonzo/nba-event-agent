"""Unit tests for src/db.py.

Only tests the error-handling behavior — the guarantee that a DB failure never
propagates to the caller. Schema and upsert SQL correctness require a live
Postgres connection and are out of scope for unit tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.db import upsert_decision, upsert_play


class TestUpsertPlaySwallowsException:
    def test_pool_error_does_not_propagate(self) -> None:
        pool = MagicMock()
        pool.execute = AsyncMock(side_effect=RuntimeError("connection refused"))

        # Must not raise — a DB outage should never crash the agent.
        asyncio.run(upsert_play(pool, {"gameId": "0041500407", "actionNumber": 1}))

    def test_missing_fields_do_not_raise(self) -> None:
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)

        # Completely empty event — all fields None — is valid input.
        asyncio.run(upsert_play(pool, {}))
        pool.execute.assert_awaited_once()


class TestUpsertDecisionSwallowsException:
    def test_pool_error_does_not_propagate(self) -> None:
        pool = MagicMock()
        pool.execute = AsyncMock(side_effect=RuntimeError("connection refused"))

        asyncio.run(
            upsert_decision(pool, "0041500407", 42, "analyzed", "Great play!", "notable")
        )

    def test_null_insight_and_severity_do_not_raise(self) -> None:
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)

        asyncio.run(upsert_decision(pool, "0041500407", 1, "skipped_routine", None, None))
        pool.execute.assert_awaited_once()
