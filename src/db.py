"""PostgreSQL persistence for play-by-play events and agent decisions."""
from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

_CREATE_PLAYS = """
CREATE TABLE IF NOT EXISTS plays (
    id            BIGSERIAL PRIMARY KEY,
    game_id       TEXT NOT NULL,
    action_number INT  NOT NULL,
    period        INT,
    clock         TEXT,
    description   TEXT,
    action_type   TEXT,
    sub_type      TEXT,
    team_id       TEXT,
    team_tricode  TEXT,
    location      TEXT,
    person_id     TEXT,
    player_name   TEXT,
    score_home    TEXT,
    score_away    TEXT,
    raw_event     JSONB NOT NULL,
    received_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (game_id, action_number)
);
"""

_CREATE_DECISIONS = """
CREATE TABLE IF NOT EXISTS agent_decisions (
    id            BIGSERIAL PRIMARY KEY,
    game_id       TEXT NOT NULL,
    action_number INT  NOT NULL,
    action        TEXT NOT NULL,
    insight       TEXT,
    severity      TEXT,
    processed_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (game_id, action_number)
);
"""

_UPSERT_PLAY = """
INSERT INTO plays (
    game_id, action_number, period, clock, description,
    action_type, sub_type, team_id, team_tricode, location,
    person_id, player_name, score_home, score_away, raw_event
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (game_id, action_number) DO NOTHING;
"""

_UPSERT_DECISION = """
INSERT INTO agent_decisions (game_id, action_number, action, insight, severity)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (game_id, action_number) DO UPDATE SET
    action       = EXCLUDED.action,
    insight      = EXCLUDED.insight,
    severity     = EXCLUDED.severity,
    processed_at = NOW();
"""


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_PLAYS)
        await conn.execute(_CREATE_DECISIONS)


def _s(v: Any) -> str | None:
    return str(v) if v is not None else None


async def upsert_play(pool: asyncpg.Pool, event: dict[str, Any]) -> None:
    try:
        await pool.execute(
            _UPSERT_PLAY,
            event.get("gameId"),
            event.get("actionNumber"),
            event.get("period"),
            event.get("clock"),
            event.get("description"),
            event.get("actionType"),
            event.get("subType"),
            _s(event.get("teamId")),
            event.get("teamTricode"),
            event.get("location"),
            _s(event.get("personId")),
            event.get("playerName"),
            event.get("scoreHome"),
            event.get("scoreAway"),
            json.dumps(event),
        )
    except Exception:
        logger.exception("upsert_play failed for action %s", event.get("actionNumber"))


async def persist_event_and_decision(
    pool: asyncpg.Pool,
    event: dict[str, Any],
    game_id: str,
    action_number: int,
    action: str,
    insight: str | None,
    severity: str | None,
) -> None:
    """Write play + decision atomically in one transaction.

    The play upsert uses ON CONFLICT DO NOTHING, so if upsert_play already
    ran at the top of _process_event the row is a no-op here. What the
    transaction buys: the decision can never land without the play being
    confirmed in the same commit, and a failure in either rolls both back.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    _UPSERT_PLAY,
                    game_id,
                    action_number,
                    event.get("period"),
                    event.get("clock"),
                    event.get("description"),
                    event.get("actionType"),
                    event.get("subType"),
                    _s(event.get("teamId")),
                    event.get("teamTricode"),
                    event.get("location"),
                    _s(event.get("personId")),
                    event.get("playerName"),
                    event.get("scoreHome"),
                    event.get("scoreAway"),
                    json.dumps(event),
                )
                await conn.execute(
                    _UPSERT_DECISION,
                    game_id,
                    action_number,
                    action,
                    insight,
                    severity,
                )
    except Exception:
        logger.exception(
            "persist_event_and_decision failed for action %s", action_number
        )


