"""Unit tests for src/nba_live_client.py.

The client is a thin wrapper around two HTTP endpoints. Tests mock the
module-level ``_session.get`` directly — we never want a unit test to hit
the real CDN.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.nba_live_client import (
    LiveClientError,
    LiveGameNotStarted,
    extract_actions,
    extract_team_ids,
    fetch_playbyplay,
    fetch_scoreboard,
    find_live_game,
)


def _mock_response(status_code: int = 200, json_payload=None, text: str = "") -> MagicMock:
    """Build a MagicMock that quacks like requests.Response."""
    r = MagicMock()
    r.status_code = status_code
    r.text = text or (str(json_payload) if json_payload else "")
    if json_payload is not None:
        r.json.return_value = json_payload
    else:
        r.json.side_effect = ValueError("no json")
    return r


# --- fetch_scoreboard ------------------------------------------------------


class TestFetchScoreboard:
    @patch("src.nba_live_client._session.get")
    def test_returns_games_list(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200,
            {
                "scoreboard": {
                    "gameDate": "2026-05-21",
                    "games": [
                        {"gameId": "0042500302", "gameStatus": 1},
                        {"gameId": "0042500401", "gameStatus": 2},
                    ],
                }
            },
        )
        games = fetch_scoreboard()
        assert len(games) == 2
        assert games[0]["gameId"] == "0042500302"

    @patch("src.nba_live_client._session.get")
    def test_empty_slate_returns_empty_list(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200, {"scoreboard": {"games": []}}
        )
        assert fetch_scoreboard() == []

    @patch("src.nba_live_client._session.get")
    def test_missing_scoreboard_key_returns_empty(self, mock_get: MagicMock) -> None:
        # Defensive: if NBA changes the response shape, don't crash with KeyError.
        mock_get.return_value = _mock_response(200, {})
        assert fetch_scoreboard() == []

    @patch("src.nba_live_client._session.get")
    def test_403_raises_client_error(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(403, text="Access Denied")
        with pytest.raises(LiveClientError, match="HTTP 403"):
            fetch_scoreboard()

    @patch("src.nba_live_client._session.get")
    def test_network_error_raises_client_error(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = requests.ConnectionError("DNS fail")
        with pytest.raises(LiveClientError, match="network error"):
            fetch_scoreboard()

    @patch("src.nba_live_client._session.get")
    def test_non_json_response_raises(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, text="<html>nope</html>")
        with pytest.raises(LiveClientError, match="non-JSON"):
            fetch_scoreboard()

    @patch("src.nba_live_client._session.get")
    def test_sends_browser_headers(self, mock_get: MagicMock) -> None:
        # The whole reason this module exists is that the CDN rejects the
        # default `requests` UA. Regression-test that we send a real UA.
        mock_get.return_value = _mock_response(200, {"scoreboard": {"games": []}})
        fetch_scoreboard()
        _, kwargs = mock_get.call_args
        headers = kwargs.get("headers", {})
        assert "Mozilla" in headers.get("User-Agent", "")
        assert "nba.com" in headers.get("Origin", "")


# --- find_live_game --------------------------------------------------------


class TestFindLiveGame:
    @patch("src.nba_live_client._session.get")
    def test_returns_first_in_progress(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200,
            {
                "scoreboard": {
                    "games": [
                        {"gameId": "A", "gameStatus": 1},
                        {"gameId": "B", "gameStatus": 2},
                        {"gameId": "C", "gameStatus": 2},  # second live, ignored
                    ]
                }
            },
        )
        game = find_live_game()
        assert game is not None
        assert game["gameId"] == "B"

    @patch("src.nba_live_client._session.get")
    def test_returns_none_if_nothing_live(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200,
            {
                "scoreboard": {
                    "games": [
                        {"gameId": "A", "gameStatus": 1},
                        {"gameId": "B", "gameStatus": 3},
                    ]
                }
            },
        )
        assert find_live_game() is None

    @patch("src.nba_live_client._session.get")
    def test_returns_none_on_empty_slate(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200, {"scoreboard": {"games": []}}
        )
        assert find_live_game() is None


# --- fetch_playbyplay ------------------------------------------------------


class TestFetchPlayByPlay:
    @patch("src.nba_live_client._session.get")
    def test_returns_full_payload(self, mock_get: MagicMock) -> None:
        payload = {
            "meta": {"version": 1},
            "game": {
                "gameId": "0042500301",
                "actions": [
                    {"actionNumber": 1, "description": "Tip-off"},
                    {"actionNumber": 2, "description": "Miss"},
                ],
            },
        }
        mock_get.return_value = _mock_response(200, payload)
        result = fetch_playbyplay("0042500301")
        assert result == payload

    @patch("src.nba_live_client._session.get")
    def test_403_maps_to_game_not_started(self, mock_get: MagicMock) -> None:
        # Critical behavior: the CDN returns 403 for not-yet-started games.
        # We want callers to handle that case distinctly (wait & retry) vs.
        # other failures (escalate).
        mock_get.return_value = _mock_response(403, text="Access Denied")
        with pytest.raises(LiveGameNotStarted):
            fetch_playbyplay("0042500302")

    @patch("src.nba_live_client._session.get")
    def test_500_raises_generic_client_error(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(500, text="server error")
        with pytest.raises(LiveClientError) as exc:
            fetch_playbyplay("0042500301")
        assert not isinstance(exc.value, LiveGameNotStarted)

    @patch("src.nba_live_client._session.get")
    def test_url_contains_game_id(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(
            200, {"game": {"gameId": "0042500301", "actions": []}}
        )
        fetch_playbyplay("0042500301")
        url = mock_get.call_args[0][0]
        assert "0042500301" in url
        assert "playbyplay" in url


# --- extract helpers -------------------------------------------------------


class TestExtractActions:
    def test_pulls_id_and_actions(self) -> None:
        gid, actions = extract_actions(
            {"game": {"gameId": "X", "actions": [{"actionNumber": 1}]}}
        )
        assert gid == "X"
        assert len(actions) == 1

    def test_missing_game_returns_empties(self) -> None:
        gid, actions = extract_actions({})
        assert gid == ""
        assert actions == []

    def test_null_game_returns_empties(self) -> None:
        # Defensive: NBA CDN sometimes returns {"game": null} momentarily.
        gid, actions = extract_actions({"game": None})
        assert gid == ""
        assert actions == []


class TestExtractTeamIds:
    def test_both_present(self) -> None:
        game = {
            "homeTeam": {"teamId": 100},
            "awayTeam": {"teamId": 200},
        }
        assert extract_team_ids(game) == (100, 200)

    def test_missing_returns_none(self) -> None:
        assert extract_team_ids({}) == (None, None)
