"""Unit tests for src/db.py.

Only tests the error-handling behavior — the guarantee that a DB failure never
propagates to the caller. Schema and upsert SQL correctness require a live
Postgres connection and are out of scope for unit tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.db import persist_event_and_decision, upsert_play


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


class TestPersistEventAndDecision:
    def _make_pool(self, execute_side_effect=None):
        """Build a mock asyncpg pool with acquire/transaction context managers."""
        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=execute_side_effect, return_value=None)

        tx_ctx = MagicMock()
        tx_ctx.__aenter__ = AsyncMock(return_value=None)
        tx_ctx.__aexit__ = AsyncMock(return_value=False)
        conn.transaction = MagicMock(return_value=tx_ctx)

        acq_ctx = MagicMock()
        acq_ctx.__aenter__ = AsyncMock(return_value=conn)
        acq_ctx.__aexit__ = AsyncMock(return_value=False)

        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acq_ctx)
        return pool, conn

    def test_pool_error_does_not_propagate(self) -> None:
        pool, _ = self._make_pool(execute_side_effect=RuntimeError("connection refused"))

        asyncio.run(
            persist_event_and_decision(
                pool, {"gameId": "0041500407"}, "0041500407", 1, "analyzed", "Great!", "notable"
            )
        )

    def test_both_writes_executed_in_one_transaction(self) -> None:
        pool, conn = self._make_pool()

        asyncio.run(
            persist_event_and_decision(
                pool,
                {"gameId": "0041500407", "period": 4},
                "0041500407",
                42,
                "analyzed",
                "Big shot.",
                "notable",
            )
        )

        # Two execute calls: one for the play, one for the decision.
        assert conn.execute.await_count == 2
        # Both happened inside a single transaction context.
        conn.transaction.assert_called_once()

    def test_null_insight_and_severity_accepted(self) -> None:
        pool, conn = self._make_pool()

        asyncio.run(
            persist_event_and_decision(
                pool, {}, "0041500407", 7, "skipped_routine", None, None
            )
        )

        assert conn.execute.await_count == 2
