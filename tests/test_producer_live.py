"""Unit tests for src/producer_live.py.

We mock both the live-client functions (to avoid HTTP) and the Kafka Producer
(to avoid needing a broker). The tests focus on:

    - resolve_game: env-var vs auto-discover behavior
    - adapt_action: the field-injection shape (gameId + location)
    - stream_game: dedup, game-end termination, error handling, signal handling
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import src.producer_live as live
from src.nba_live_client import LiveClientError, LiveGameNotStarted


# --- resolve_game ----------------------------------------------------------


class TestResolveGame:
    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.find_live_game")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "")
    def test_auto_discover_returns_first_live(
        self, mock_find: MagicMock, mock_scoreboard: MagicMock
    ) -> None:
        # Auto-discover path: no NBA_GAME_ID set → return whatever
        # find_live_game returns.
        mock_scoreboard.return_value = [{"gameId": "A"}]
        mock_find.return_value = {"gameId": "B", "gameStatus": 2}
        result = live.resolve_game()
        assert result["gameId"] == "B"

    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.find_live_game")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "")
    def test_auto_discover_with_no_live_games_raises(
        self, mock_find: MagicMock, mock_scoreboard: MagicMock
    ) -> None:
        mock_scoreboard.return_value = [{"gameId": "A", "gameStatus": 1}]
        mock_find.return_value = None
        with pytest.raises(RuntimeError, match="no in-progress"):
            live.resolve_game()

    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "0042500302")
    def test_explicit_in_progress_returns_match(
        self, mock_scoreboard: MagicMock
    ) -> None:
        mock_scoreboard.return_value = [
            {"gameId": "0042500301", "gameStatus": 3},
            {"gameId": "0042500302", "gameStatus": 2},
        ]
        result = live.resolve_game()
        assert result["gameId"] == "0042500302"

    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "0042500302")
    def test_explicit_unstarted_raises_clear_error(
        self, mock_scoreboard: MagicMock
    ) -> None:
        mock_scoreboard.return_value = [
            {"gameId": "0042500302", "gameStatus": 1, "gameStatusText": "8:00 pm ET"}
        ]
        with pytest.raises(RuntimeError, match="hasn't started"):
            live.resolve_game()

    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "0042500302")
    def test_explicit_already_final_raises(
        self, mock_scoreboard: MagicMock
    ) -> None:
        mock_scoreboard.return_value = [
            {"gameId": "0042500302", "gameStatus": 3}
        ]
        with pytest.raises(RuntimeError, match="already final"):
            live.resolve_game()

    @patch("src.producer_live.fetch_scoreboard")
    @patch("src.producer_live.EXPLICIT_GAME_ID", "9999999999")
    def test_explicit_unknown_game_id_raises(
        self, mock_scoreboard: MagicMock
    ) -> None:
        mock_scoreboard.return_value = [
            {"gameId": "0042500302", "gameStatus": 2}
        ]
        with pytest.raises(RuntimeError, match="not in today"):
            live.resolve_game()


# --- adapt_action ----------------------------------------------------------


class TestAdaptAction:
    def test_injects_game_id(self) -> None:
        out = live.adapt_action(
            {"actionNumber": 1, "teamId": 100},
            game_id="0042500302",
            home_team_id=100,
            away_team_id=200,
        )
        assert out["gameId"] == "0042500302"

    def test_home_team_id_maps_to_h(self) -> None:
        out = live.adapt_action(
            {"actionNumber": 1, "teamId": 100},
            game_id="X",
            home_team_id=100,
            away_team_id=200,
        )
        assert out["location"] == "h"

    def test_away_team_id_maps_to_v(self) -> None:
        out = live.adapt_action(
            {"actionNumber": 1, "teamId": 200},
            game_id="X",
            home_team_id=100,
            away_team_id=200,
        )
        assert out["location"] == "v"

    def test_unknown_team_id_maps_to_empty(self) -> None:
        # Some events (jump ball, period start) have no team — preserve that.
        out = live.adapt_action(
            {"actionNumber": 1, "teamId": None},
            game_id="X",
            home_team_id=100,
            away_team_id=200,
        )
        assert out["location"] == ""

    def test_does_not_mutate_input(self) -> None:
        original = {"actionNumber": 1, "teamId": 100}
        out = live.adapt_action(
            original, game_id="X", home_team_id=100, away_team_id=200
        )
        assert "gameId" not in original
        assert "location" not in original
        assert out is not original

    def test_preserves_existing_fields(self) -> None:
        out = live.adapt_action(
            {
                "actionNumber": 42,
                "teamId": 100,
                "description": "Made 3PT",
                "scoreHome": "12",
                "scoreAway": "10",
            },
            game_id="X",
            home_team_id=100,
            away_team_id=200,
        )
        assert out["description"] == "Made 3PT"
        assert out["scoreHome"] == "12"
        assert out["actionNumber"] == 42


# --- stream_game -----------------------------------------------------------


def _game(game_id: str = "0042500302", status: int = 2) -> dict:
    return {
        "gameId": game_id,
        "gameStatus": status,
        "homeTeam": {"teamId": 100, "teamTricode": "NYK"},
        "awayTeam": {"teamId": 200, "teamTricode": "CLE"},
    }


def _pbp(action_numbers: list[int]) -> dict:
    return {
        "game": {
            "gameId": "0042500302",
            "actions": [
                {
                    "actionNumber": n,
                    "teamId": 100 if n % 2 == 0 else 200,
                    "description": f"Play {n}",
                    "period": 1,
                    "clock": "PT12M00.00S",
                    "scoreHome": "0",
                    "scoreAway": "0",
                }
                for n in action_numbers
            ],
        }
    }


class TestStreamGame:
    def setup_method(self) -> None:
        # Reset the module-level _running flag at the start of each test —
        # signal-handler tests flip it and we don't want bleed-over.
        live._running = True

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_publishes_each_new_action_once(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # Two poll cycles: first sees actions 1-3, second sees 1-5 (so 4-5
        # are new). Third cycle reports game final so we exit.
        mock_scoreboard.side_effect = [
            [_game(status=2)],
            [_game(status=2)],
            [_game(status=3)],
        ]
        mock_pbp.side_effect = [
            _pbp([1, 2, 3]),
            _pbp([1, 2, 3, 4, 5]),
            _pbp([1, 2, 3, 4, 5]),
        ]

        producer = MagicMock()
        total = live.stream_game(producer, _game(), poll_seconds=0)

        # 5 unique actions across all polls, each published once.
        assert total == 5
        assert producer.produce.call_count == 5

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_exits_on_game_final(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # Single cycle, game already final.
        mock_scoreboard.return_value = [_game(status=3)]
        mock_pbp.return_value = _pbp([1])
        producer = MagicMock()
        live.stream_game(producer, _game(), poll_seconds=0)
        # One poll only — we shouldn't loop again.
        assert mock_pbp.call_count == 1

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_pbp_fetch_error_retries(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # First poll raises a transient error; second succeeds; third ends game.
        mock_scoreboard.side_effect = [
            [_game(status=2)],
            [_game(status=2)],
            [_game(status=3)],
        ]
        mock_pbp.side_effect = [
            LiveClientError("transient network blip"),
            _pbp([1, 2]),
            _pbp([1, 2]),
        ]
        producer = MagicMock()
        total = live.stream_game(producer, _game(), poll_seconds=0)
        assert total == 2  # only the successful poll's actions
        assert producer.produce.call_count == 2

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_game_not_started_keeps_polling(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # PBP returns "not started" until cycle 3, then we get plays + final.
        mock_scoreboard.side_effect = [
            [_game(status=1)],  # still scheduled
            [_game(status=2)],  # tipped off
            [_game(status=3)],  # final
        ]
        mock_pbp.side_effect = [
            LiveGameNotStarted("not yet"),
            _pbp([1, 2]),
            _pbp([1, 2]),
        ]
        producer = MagicMock()
        total = live.stream_game(producer, _game(), poll_seconds=0)
        assert total == 2

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_signal_stops_loop(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # Simulate SIGINT mid-stream by flipping _running on the 2nd scoreboard
        # poll. The current iteration finishes (so PBP gets fetched twice
        # total), then the while-check fails and we exit. Key behavior under
        # test: the loop terminates without ever seeing gameStatus=3.
        scoreboard_calls = {"n": 0}

        def _flip_on_second_call():
            scoreboard_calls["n"] += 1
            if scoreboard_calls["n"] == 2:
                live._running = False
            return [_game(status=2)]

        mock_scoreboard.side_effect = _flip_on_second_call
        mock_pbp.return_value = _pbp([1])
        producer = MagicMock()
        live.stream_game(producer, _game(), poll_seconds=0)
        # Loop terminates in finite time: 2 iterations (signal flipped during
        # iter 2's scoreboard fetch, iter 2 finishes, then while-check exits).
        assert mock_pbp.call_count == 2
        # restore for other tests
        live._running = True

    @patch("src.producer_live.time.sleep")
    @patch("src.producer_live.fetch_playbyplay")
    @patch("src.producer_live.fetch_scoreboard")
    def test_published_payloads_have_injected_fields(
        self,
        mock_scoreboard: MagicMock,
        mock_pbp: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        # Verify the bytes we publish include gameId + location, the two
        # fields the agent depends on but the live feed doesn't ship.
        import json as _json

        mock_scoreboard.side_effect = [
            [_game(status=2)],
            [_game(status=3)],
        ]
        mock_pbp.side_effect = [_pbp([1]), _pbp([1])]
        producer = MagicMock()
        live.stream_game(producer, _game(), poll_seconds=0)

        # First positional arg is topic; second is the encoded payload.
        topic, payload = producer.produce.call_args[0]
        adapted = _json.loads(payload)
        assert adapted["gameId"] == "0042500302"
        assert adapted["location"] in ("h", "v")
