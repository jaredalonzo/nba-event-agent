"""Unit tests for src/agent.py — parse_clock and GameContextTracker.

Pure unit tests; no Kafka, no nba_api, no network. The graph-level tests
called out in CLAUDE.md's TODOs will land once the LangGraph agent is in
place (M3+).
"""

from __future__ import annotations

import pytest

from src.agent import GameContextTracker, parse_clock


# --- Fixtures / helpers -----------------------------------------------------

def make_event(**overrides) -> dict:
    """Build a minimal PlayByPlayV3-shaped event with sensible defaults.

    Tests pass ``**overrides`` for whatever fields they care about.
    """
    base = {
        "gameId": "0041500407",
        "actionNumber": 1,
        "clock": "PT12M00.00S",
        "period": 1,
        "teamId": 0,
        "teamTricode": "",
        "personId": 0,
        "playerName": "",
        "scoreHome": "",
        "scoreAway": "",
        "location": "",
        "description": "",
        "actionType": "",
        "subType": "",
    }
    base.update(overrides)
    return base


# --- parse_clock ------------------------------------------------------------

class TestParseClock:
    @pytest.mark.parametrize(
        "pt,expected",
        [
            ("PT12M00.00S", "12:00"),
            ("PT07M30.50S", "7:30"),
            ("PT00M25.20S", "0:25"),
            ("PT00M00.00S", "0:00"),
        ],
    )
    def test_standard_durations(self, pt: str, expected: str) -> None:
        assert parse_clock(pt) == expected

    def test_none_returns_zero(self) -> None:
        assert parse_clock(None) == "00:00"

    def test_empty_string_returns_zero(self) -> None:
        assert parse_clock("") == "00:00"

    def test_non_pt_string_passes_through(self) -> None:
        # Should not crash on unexpected input; just returns the original.
        assert parse_clock("garbage") == "garbage"


# --- GameContextTracker -----------------------------------------------------

class TestGameContextTrackerInitial:
    def test_initial_snapshot_defaults(self) -> None:
        snap = GameContextTracker().snapshot()
        assert snap["game_id"] is None
        assert snap["period"] == 1
        assert snap["clock"] == "12:00"
        assert snap["score_home"] == 0
        assert snap["score_away"] == 0
        assert snap["score_margin"] == 0
        assert snap["home_team"] == ""
        assert snap["away_team"] == ""
        assert snap["last_scoring_plays"] == []
        assert snap["player_fouls"] == {}


class TestGameContextTrackerIdentity:
    def test_game_id_set_from_first_event(self) -> None:
        t = GameContextTracker()
        t.update(make_event(gameId="0041500407"))
        assert t.game_id == "0041500407"

    def test_game_id_not_overwritten(self) -> None:
        # Defensive: later events with a different gameId shouldn't clobber.
        t = GameContextTracker()
        t.update(make_event(gameId="0041500407"))
        t.update(make_event(gameId="0099999999"))
        assert t.game_id == "0041500407"

    def test_team_tricodes_captured_from_first_match(self) -> None:
        t = GameContextTracker()
        t.update(make_event(location="h", teamTricode="GSW"))
        t.update(make_event(location="v", teamTricode="CLE"))
        snap = t.snapshot()
        assert snap["home_team"] == "GSW"
        assert snap["away_team"] == "CLE"


class TestGameContextTrackerScoring:
    def test_first_score_creates_scoring_play(self) -> None:
        t = GameContextTracker()
        t.update(
            make_event(
                actionNumber=10,
                location="v",
                teamTricode="CLE",
                playerName="Irving",
                scoreHome="0",
                scoreAway="2",
                description="Irving 3' Layup",
            )
        )
        snap = t.snapshot()
        assert snap["score_home"] == 0
        assert snap["score_away"] == 2
        assert snap["score_margin"] == -2  # away leading
        assert len(snap["last_scoring_plays"]) == 1
        play = snap["last_scoring_plays"][0]
        assert play["team"] == "CLE"
        assert play["player"] == "Irving"
        assert play["score_away"] == 2

    def test_empty_score_strings_ignored(self) -> None:
        # Pre-tipoff and pre-first-basket events arrive with scoreHome="".
        t = GameContextTracker()
        t.update(make_event(scoreHome="", scoreAway=""))
        snap = t.snapshot()
        assert snap["score_home"] == 0
        assert snap["score_away"] == 0
        assert snap["last_scoring_plays"] == []

    def test_unchanged_score_not_recorded(self) -> None:
        # If a later event reports the same running score, no new scoring play.
        t = GameContextTracker()
        t.update(make_event(scoreHome="2", scoreAway="0", teamTricode="GSW"))
        t.update(make_event(scoreHome="2", scoreAway="0", actionType="Rebound"))
        assert len(t.last_scoring_plays) == 1

    def test_scoring_plays_deque_evicts_oldest(self) -> None:
        # last_scoring_plays is a deque(maxlen=5).
        t = GameContextTracker()
        for i in range(1, 8):
            t.update(
                make_event(
                    actionNumber=i,
                    scoreHome=str(i * 2),
                    scoreAway="0",
                    teamTricode="GSW",
                    description=f"Play {i}",
                )
            )
        snap = t.snapshot()
        assert len(snap["last_scoring_plays"]) == 5
        assert snap["last_scoring_plays"][0]["description"] == "Play 3"
        assert snap["last_scoring_plays"][-1]["description"] == "Play 7"


class TestGameContextTrackerFouls:
    def test_foul_counted_per_player(self) -> None:
        t = GameContextTracker()
        t.update(make_event(personId=201939, actionType="Foul", subType="Personal"))
        t.update(make_event(personId=201939, actionType="Foul", subType="Personal"))
        t.update(make_event(personId=2544, actionType="Foul", subType="Shooting"))
        assert t.player_fouls == {201939: 2, 2544: 1}

    def test_foul_match_is_case_insensitive(self) -> None:
        t = GameContextTracker()
        t.update(make_event(personId=42, actionType="FOUL"))
        t.update(make_event(personId=42, actionType="Foul"))
        t.update(make_event(personId=42, actionType="foul"))
        assert t.player_fouls == {42: 3}

    def test_non_foul_events_dont_increment(self) -> None:
        t = GameContextTracker()
        t.update(make_event(personId=42, actionType="Substitution"))
        t.update(make_event(personId=42, actionType="Made Shot"))
        assert t.player_fouls == {}

    def test_foul_with_no_personid_ignored(self) -> None:
        # Defensive: some foul-adjacent events (e.g., team technicals) may have
        # personId=0 from the producer's NaN-fill. Don't pollute the counts.
        t = GameContextTracker()
        t.update(make_event(personId=0, actionType="Foul"))
        assert t.player_fouls == {}


class TestGameContextTrackerTime:
    def test_period_advances(self) -> None:
        t = GameContextTracker()
        for p in [1, 2, 3, 4]:
            t.update(make_event(period=p))
        assert t.period == 4

    def test_clock_updated_each_event(self) -> None:
        t = GameContextTracker()
        t.update(make_event(clock="PT12M00.00S"))
        assert t.clock == "12:00"
        t.update(make_event(clock="PT07M30.00S"))
        assert t.clock == "7:30"


class TestGameContextTrackerReturnValue:
    def test_update_returns_a_snapshot_dict(self) -> None:
        t = GameContextTracker()
        result = t.update(make_event(scoreHome="2", scoreAway="0"))
        assert isinstance(result, dict)
        assert result["score_home"] == 2
        # Should be defensively copied (mutating it shouldn't affect internals).
        result["score_home"] = 999
        assert t.score_home == 2
