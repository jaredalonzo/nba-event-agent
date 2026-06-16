"""Unit tests for src/team_context.py.

Covers:
    _is_fresh         — pure TTL check (no I/O)
    TeamContextProvider.get — cache miss/hit/expiry, disk write, empty tricode
    TeamContextProvider._fetch_from_api — nba_api normalization (mocked)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.team_context import TeamContextProvider, _is_fresh


# --- _is_fresh ---------------------------------------------------------------


class TestIsFresh:
    def test_recent_entry_is_fresh(self) -> None:
        entry = {"fetched_at": datetime.now(tz=timezone.utc).isoformat()}
        assert _is_fresh(entry) is True

    def test_entry_just_past_ttl_is_stale(self) -> None:
        old = (datetime.now(tz=timezone.utc) - timedelta(hours=25)).isoformat()
        assert _is_fresh({"fetched_at": old}) is False

    def test_missing_fetched_at_is_stale(self) -> None:
        assert _is_fresh({}) is False

    def test_invalid_timestamp_is_stale(self) -> None:
        assert _is_fresh({"fetched_at": "not-a-date"}) is False


# --- TeamContextProvider -----------------------------------------------------


@pytest.fixture
def provider(tmp_path, monkeypatch):
    """Fresh provider backed by a temp cache file for each test."""
    cache_file = tmp_path / "team_context.json"
    monkeypatch.setattr("src.team_context._CACHE_PATH", cache_file)
    return TeamContextProvider()


def _stub_fetch(data: dict):
    """Return a mock _fetch_from_api that returns ``data``."""
    return MagicMock(return_value=data)


_FAKE_CTX = {"coach": "JJ Redick", "record": "53-29", "seed": 4, "roster": {"2544": "LeBron James"}}


class TestTeamContextProviderGet:
    def test_cache_miss_calls_fetch_api(self, provider, monkeypatch) -> None:
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        result = provider.get("LAL", "2026-06-15")
        provider._fetch_from_api.assert_called_once_with("LAL")
        assert result["coach"] == "JJ Redick"

    def test_cache_hit_within_ttl_avoids_refetch(self, provider, monkeypatch) -> None:
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        provider.get("LAL", "2026-06-15")
        provider.get("LAL", "2026-06-15")
        assert provider._fetch_from_api.call_count == 1

    def test_stale_entry_triggers_refetch(self, provider, monkeypatch) -> None:
        stale_ts = (datetime.now(tz=timezone.utc) - timedelta(hours=25)).isoformat()
        provider._cache["LAL::2026-06-15"] = {"coach": "old coach", "fetched_at": stale_ts}
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        result = provider.get("LAL", "2026-06-15")
        provider._fetch_from_api.assert_called_once_with("LAL")
        assert result["coach"] == "JJ Redick"

    def test_result_written_to_disk_on_miss(self, provider, tmp_path, monkeypatch) -> None:
        cache_file = tmp_path / "team_context.json"
        monkeypatch.setattr("src.team_context._CACHE_PATH", cache_file)
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        provider.get("LAL", "2026-06-15")
        assert cache_file.exists()
        on_disk = json.loads(cache_file.read_text())
        assert "LAL::2026-06-15" in on_disk

    def test_fetched_at_added_to_cached_entry(self, provider, monkeypatch) -> None:
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        provider.get("LAL", "2026-06-15")
        assert "fetched_at" in provider._cache["LAL::2026-06-15"]

    def test_empty_tricode_returns_empty_dict(self, provider) -> None:
        result = provider.get("", "2026-06-15")
        assert result == {}

    def test_different_game_dates_cached_separately(self, provider, monkeypatch) -> None:
        monkeypatch.setattr(provider, "_fetch_from_api", _stub_fetch(_FAKE_CTX))
        provider.get("LAL", "2026-06-14")
        provider.get("LAL", "2026-06-15")
        assert provider._fetch_from_api.call_count == 2
        assert "LAL::2026-06-14" in provider._cache
        assert "LAL::2026-06-15" in provider._cache


# --- _fetch_from_api ---------------------------------------------------------


class TestFetchFromApi:
    @patch("src.team_context.commonteamroster.CommonTeamRoster")
    @patch("src.team_context.leaguestandingsv3.LeagueStandingsV3")
    @patch("src.team_context.nba_teams_static.find_team_by_abbreviation")
    def test_normalizes_full_response(
        self, mock_static, mock_standings_cls, mock_roster_cls
    ) -> None:
        mock_static.return_value = {"id": 1610612747, "abbreviation": "LAL"}
        mock_standings_cls.return_value.get_normalized_dict.return_value = {
            "Standings": [{"TeamID": 1610612747, "Record": "53-29", "PlayoffRank": 4}]
        }
        mock_roster_cls.return_value.get_normalized_dict.return_value = {
            "CommonTeamRoster": [
                {"PLAYER_ID": 2544, "PLAYER": "LeBron James"},
                {"PLAYER_ID": 203076, "PLAYER": "Anthony Davis"},
            ],
            "Coaches": [
                {"COACH_NAME": "JJ Redick", "COACH_TYPE": "Head Coach"},
                {"COACH_NAME": "Phil Handy", "COACH_TYPE": "Assistant Coach"},
            ],
        }

        result = TeamContextProvider()._fetch_from_api("LAL")

        assert result["coach"] == "JJ Redick"
        assert result["record"] == "53-29"
        assert result["seed"] == 4
        assert result["roster"]["2544"] == "LeBron James"
        assert result["roster"]["203076"] == "Anthony Davis"

    @patch("src.team_context.nba_teams_static.find_team_by_abbreviation")
    def test_unknown_tricode_returns_empty_dict(self, mock_static) -> None:
        mock_static.return_value = None
        result = TeamContextProvider()._fetch_from_api("XYZ")
        assert result == {"coach": None, "record": None, "seed": None, "roster": {}}

    @patch("src.team_context.commonteamroster.CommonTeamRoster")
    @patch("src.team_context.leaguestandingsv3.LeagueStandingsV3")
    @patch("src.team_context.nba_teams_static.find_team_by_abbreviation")
    def test_team_absent_from_standings_gives_none_record(
        self, mock_static, mock_standings_cls, mock_roster_cls
    ) -> None:
        mock_static.return_value = {"id": 9999}
        mock_standings_cls.return_value.get_normalized_dict.return_value = {"Standings": []}
        mock_roster_cls.return_value.get_normalized_dict.return_value = {
            "CommonTeamRoster": [], "Coaches": []
        }
        result = TeamContextProvider()._fetch_from_api("XYZ")
        assert result["record"] is None
        assert result["seed"] is None

    @patch("src.team_context.commonteamroster.CommonTeamRoster")
    @patch("src.team_context.leaguestandingsv3.LeagueStandingsV3")
    @patch("src.team_context.nba_teams_static.find_team_by_abbreviation")
    def test_only_head_coach_extracted(
        self, mock_static, mock_standings_cls, mock_roster_cls
    ) -> None:
        mock_static.return_value = {"id": 1610612747}
        mock_standings_cls.return_value.get_normalized_dict.return_value = {"Standings": []}
        mock_roster_cls.return_value.get_normalized_dict.return_value = {
            "CommonTeamRoster": [],
            "Coaches": [
                {"COACH_NAME": "Assistant A", "COACH_TYPE": "Assistant Coach"},
                {"COACH_NAME": "JJ Redick", "COACH_TYPE": "Head Coach"},
                {"COACH_NAME": "Assistant B", "COACH_TYPE": "Assistant Coach"},
            ],
        }
        result = TeamContextProvider()._fetch_from_api("LAL")
        assert result["coach"] == "JJ Redick"

    @patch("src.team_context.commonteamroster.CommonTeamRoster")
    @patch("src.team_context.leaguestandingsv3.LeagueStandingsV3")
    @patch("src.team_context.nba_teams_static.find_team_by_abbreviation")
    def test_no_head_coach_in_response_gives_none(
        self, mock_static, mock_standings_cls, mock_roster_cls
    ) -> None:
        mock_static.return_value = {"id": 1610612747}
        mock_standings_cls.return_value.get_normalized_dict.return_value = {"Standings": []}
        mock_roster_cls.return_value.get_normalized_dict.return_value = {
            "CommonTeamRoster": [],
            "Coaches": [{"COACH_NAME": "Assistant A", "COACH_TYPE": "Assistant Coach"}],
        }
        result = TeamContextProvider()._fetch_from_api("LAL")
        assert result["coach"] is None
