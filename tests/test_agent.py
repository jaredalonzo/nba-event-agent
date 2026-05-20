"""Unit tests for src/agent.py.

Covers:
    - parse_clock (pure function)
    - GameContextTracker (stateful, no I/O)
    - Action enum (canonical string values)
    - classify_event node (with LLM mocked)
    - Compiled graph end-to-end (with LLM mocked)

No real Kafka, nba_api, or Anthropic calls happen in this file. All LLM
interactions are patched out at ``src.agent._llm``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from src.agent import GameContextTracker, _graph, classify_event, parse_clock
from src.state import Action


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


# --- Helpers for graph-related tests ----------------------------------------


def make_state(**overrides) -> dict:
    """Build a minimal AgentState dict for graph/node tests."""
    base = {
        "event": {
            "actionNumber": 42,
            "description": "Test event description",
            "actionType": "Substitution",
        },
        "game_context": {
            "period": 1,
            "clock": "12:00",
            "score_home": 0,
            "score_away": 0,
            "home_team": "GSW",
            "away_team": "CLE",
        },
        "messages": [],
        "action": Action.SKIPPED_OTHER,
        "insight": None,
    }
    base.update(overrides)
    return base


# --- Action enum sanity -----------------------------------------------------


class TestActionEnum:
    def test_canonical_values(self) -> None:
        # These string values are used in log lines and (in M5) persisted to
        # insights.jsonl, so renames should fail the test loudly.
        assert Action.ANALYZED.value == "analyzed"
        assert Action.SKIPPED_EARLY_Q.value == "skipped_early_q"
        assert Action.SKIPPED_ROUTINE.value == "skipped_routine"
        assert Action.SKIPPED_OTHER.value == "skipped_other"

    def test_action_is_str_compatible(self) -> None:
        # Action inherits from str so json.dumps and == "literal" both work.
        assert Action.ANALYZED == "analyzed"


# --- classify_event node ----------------------------------------------------


class TestClassifyEvent:
    @patch("src.agent._llm")
    def test_returns_action_and_appended_message(self, mock_llm: MagicMock) -> None:
        mock_llm.invoke.return_value = AIMessage(content="skip")
        result = classify_event(make_state())

        assert result["action"] == Action.SKIPPED_OTHER
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert result["messages"][0].content == "skip"

    @patch("src.agent._llm")
    def test_llm_called_with_system_then_user(self, mock_llm: MagicMock) -> None:
        mock_llm.invoke.return_value = AIMessage(content="skip")
        classify_event(make_state())

        mock_llm.invoke.assert_called_once()
        call_messages = mock_llm.invoke.call_args[0][0]
        assert len(call_messages) == 2
        assert isinstance(call_messages[0], SystemMessage)
        assert isinstance(call_messages[1], HumanMessage)

    @patch("src.agent._llm")
    def test_user_message_includes_event_and_context(
        self, mock_llm: MagicMock
    ) -> None:
        mock_llm.invoke.return_value = AIMessage(content="skip")
        state = make_state(
            event={
                "actionNumber": 207,
                "description": "Irving Free Throw 1 of 1 (9 PTS)",
                "actionType": "Free Throw",
            },
            game_context={
                "period": 2,
                "clock": "3:59",
                "score_home": 38,
                "score_away": 38,
                "home_team": "GSW",
                "away_team": "CLE",
            },
        )
        classify_event(state)

        user_msg = mock_llm.invoke.call_args[0][0][1].content
        assert "207" in user_msg
        assert "Irving Free Throw" in user_msg
        assert "Free Throw" in user_msg
        assert "3:59" in user_msg
        assert "GSW" in user_msg
        assert "CLE" in user_msg
        assert "38" in user_msg

    @patch("src.agent._llm")
    def test_m3_always_returns_skipped_other(self, mock_llm: MagicMock) -> None:
        # M3 stub behavior: action is hard-coded to SKIPPED_OTHER regardless of
        # the model's response. M4 will replace this with real branching, and
        # this test should be updated/deleted at that point.
        mock_llm.invoke.return_value = AIMessage(content="this is not 'skip'")
        result = classify_event(make_state())
        assert result["action"] == Action.SKIPPED_OTHER


# --- Compiled graph end-to-end ---------------------------------------------


class TestCompiledGraph:
    @patch("src.agent._llm")
    def test_invoke_sets_action(self, mock_llm: MagicMock) -> None:
        mock_llm.invoke.return_value = AIMessage(content="skip")
        result = _graph.invoke(make_state())
        assert result["action"] == Action.SKIPPED_OTHER

    @patch("src.agent._llm")
    def test_invoke_appends_message_via_reducer(self, mock_llm: MagicMock) -> None:
        # add_messages should append rather than overwrite. Start with a prior
        # message and verify both are present in the final state.
        mock_llm.invoke.return_value = AIMessage(content="skip")
        prior = AIMessage(content="previous turn")
        result = _graph.invoke(make_state(messages=[prior]))

        assert len(result["messages"]) == 2
        assert result["messages"][0].content == "previous turn"
        assert result["messages"][1].content == "skip"

    @patch("src.agent._llm")
    def test_invoke_preserves_event_and_game_context(
        self, mock_llm: MagicMock
    ) -> None:
        mock_llm.invoke.return_value = AIMessage(content="skip")
        state = make_state()
        result = _graph.invoke(state)

        assert result["event"] == state["event"]
        assert result["game_context"] == state["game_context"]
        assert result["insight"] is None
