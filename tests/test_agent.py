"""Unit tests for src/agent.py.

Covers:
    - parse_clock (pure function)
    - GameContextTracker (stateful, no I/O)
    - Action enum (canonical string values)
    - _parse_action_from_text (pure function)
    - route_after_classify (pure function)
    - finalize node (pure function)
    - _parse_narrator_response (pure function)
    - generate_insight node (LLM mocked)

We don't unit-test classify_event itself because patching the bound LLM
(_llm_with_tools) cleanly is awkward, and the graph wiring is covered by
running the agent end-to-end. The pure helpers around it are where the
testable logic lives.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.agent import (
    GameContextTracker,
    _parse_action_from_text,
    _parse_narrator_response,
    _process_event,
    _summarize_tool_results,
    finalize,
    generate_insight,
    parse_clock,
    route_after_classify,
)
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
        "severity": None,
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


# --- _parse_action_from_text ------------------------------------------------


class TestParseActionFromText:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("ANALYZE: tied game in Q4", Action.ANALYZED),
            ("analyze: lowercase too", Action.ANALYZED),
            ("READY: forgiving fallback", Action.ANALYZED),
            ("SKIP_ROUTINE: substitution", Action.SKIPPED_ROUTINE),
            ("SKIPPED_ROUTINE: alternative spelling", Action.SKIPPED_ROUTINE),
            ("SKIP_EARLY: Q1 free throw", Action.SKIPPED_EARLY_Q),
            ("SKIPPED_EARLY: alt spelling", Action.SKIPPED_EARLY_Q),
            ("SKIP_OTHER: catch-all", Action.SKIPPED_OTHER),
            ("SKIP: bare skip fallback", Action.SKIPPED_OTHER),
        ],
    )
    def test_known_prefixes(self, text: str, expected: Action) -> None:
        assert _parse_action_from_text(text) == expected

    def test_empty_string_defaults_to_skipped_other(self) -> None:
        assert _parse_action_from_text("") == Action.SKIPPED_OTHER

    def test_none_defaults_to_skipped_other(self) -> None:
        assert _parse_action_from_text(None) == Action.SKIPPED_OTHER  # type: ignore[arg-type]

    def test_unknown_prefix_defaults_to_skipped_other(self) -> None:
        # Garbage / non-conforming model output falls back safely.
        assert _parse_action_from_text("I think we should look at this") == Action.SKIPPED_OTHER

    def test_leading_whitespace_handled(self) -> None:
        assert _parse_action_from_text("   ANALYZE: ok") == Action.ANALYZED


# --- route_after_classify --------------------------------------------------


class TestRouteAfterClassify:
    def test_tool_calls_route_to_call_tools(self) -> None:
        msg = AIMessage(
            content="",
            tool_calls=[
                {"name": "get_player_stats", "args": {}, "id": "t1", "type": "tool_call"}
            ],
        )
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "call_tools"

    def test_analyze_prefix_routes_to_generate_insight(self) -> None:
        # ANALYZE: is the model's signal that the event is notable and we
        # should generate a narrative.
        msg = AIMessage(content="ANALYZE: notable late-game play")
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "generate_insight"

    def test_ready_prefix_also_routes_to_generate_insight(self) -> None:
        # READY: is accepted as an alias for ANALYZE (forgiving parsing).
        msg = AIMessage(content="READY: late-game scoring run")
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "generate_insight"

    def test_skip_routine_routes_to_finalize(self) -> None:
        msg = AIMessage(content="SKIP_ROUTINE: substitution")
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "finalize"

    def test_skip_early_routes_to_finalize(self) -> None:
        msg = AIMessage(content="SKIP_EARLY: Q1 free throw")
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "finalize"

    def test_empty_tool_calls_list_routes_to_finalize(self) -> None:
        # Edge case: AIMessage with explicitly empty tool_calls list.
        msg = AIMessage(content="SKIP_ROUTINE: subs", tool_calls=[])
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "finalize"

    def test_non_ai_message_routes_to_finalize(self) -> None:
        # Defensive: if somehow the last message isn't an AIMessage, don't
        # try to route to call_tools.
        state = make_state(messages=[HumanMessage(content="just user")])
        assert route_after_classify(state) == "finalize"

    def test_unparseable_text_routes_to_finalize(self) -> None:
        # Garbage from the model shouldn't accidentally trigger the analyze path.
        msg = AIMessage(content="I'm not sure what to do here")
        state = make_state(messages=[msg])
        assert route_after_classify(state) == "finalize"


# --- finalize node ---------------------------------------------------------


class TestFinalize:
    def test_finalize_sets_action_from_last_message(self) -> None:
        state = make_state(messages=[AIMessage(content="ANALYZE: it's notable")])
        result = finalize(state)
        assert result == {"action": Action.ANALYZED}

    def test_finalize_with_skip_routine(self) -> None:
        state = make_state(messages=[AIMessage(content="SKIP_ROUTINE: just a sub")])
        result = finalize(state)
        assert result == {"action": Action.SKIPPED_ROUTINE}

    def test_finalize_falls_back_on_unparseable(self) -> None:
        state = make_state(messages=[AIMessage(content="hmm not sure")])
        result = finalize(state)
        assert result == {"action": Action.SKIPPED_OTHER}


# --- _parse_narrator_response ----------------------------------------------


class TestParseNarratorResponse:
    def test_canonical_format(self) -> None:
        text = (
            "SEVERITY: critical\n"
            "INSIGHT: LeBron James just tied Game 7 at 89 with a driving layup."
        )
        severity, insight = _parse_narrator_response(text)
        assert severity == "critical"
        assert "LeBron" in insight
        assert insight.endswith("layup.")

    def test_severity_case_insensitive(self) -> None:
        # Narrator may return upper or mixed case for the severity value.
        severity, _ = _parse_narrator_response("SEVERITY: Notable\nINSIGHT: foo")
        assert severity == "notable"

    def test_multiline_insight_is_joined(self) -> None:
        # Narrator may wrap the narrative across multiple lines.
        text = (
            "SEVERITY: notable\n"
            "INSIGHT: First sentence here.\n"
            "Second sentence continues.\n"
            "Third sentence wraps it."
        )
        _, insight = _parse_narrator_response(text)
        assert "First sentence" in insight
        assert "Second sentence" in insight
        assert "Third sentence" in insight

    def test_unparseable_falls_back_to_raw_text(self) -> None:
        # If the narrator ignores the format, we still want SOMETHING in the
        # insight rather than dropping the work entirely.
        severity, insight = _parse_narrator_response("Just a raw sentence.")
        assert severity == "notable"  # default
        assert insight == "Just a raw sentence."

    def test_missing_severity_defaults_to_notable(self) -> None:
        _, insight = _parse_narrator_response("INSIGHT: only the insight line")
        assert "only the insight line" in insight
        severity, _ = _parse_narrator_response("INSIGHT: only the insight line")
        assert severity == "notable"

    def test_empty_string(self) -> None:
        severity, insight = _parse_narrator_response("")
        assert severity == "notable"
        assert insight == ""


# --- _summarize_tool_results -----------------------------------------------


class TestSummarizeToolResults:
    def test_no_tool_messages_returns_empty(self) -> None:
        assert _summarize_tool_results([]) == ""
        assert _summarize_tool_results([AIMessage(content="hi")]) == ""

    def test_includes_tool_name_and_content(self) -> None:
        messages = [
            ToolMessage(
                content='{"points": 27}', name="get_player_stats", tool_call_id="t1"
            ),
            ToolMessage(
                content='{"summary": "GSW on a run"}',
                name="analyze_momentum",
                tool_call_id="t2",
            ),
        ]
        result = _summarize_tool_results(messages)
        assert "get_player_stats" in result
        assert "analyze_momentum" in result
        assert "points" in result
        assert "GSW on a run" in result


# --- generate_insight node -------------------------------------------------


class TestGenerateInsight:
    @patch("src.agent._narrator")
    def test_returns_insight_severity_and_action(
        self, mock_narrator: MagicMock
    ) -> None:
        mock_narrator.invoke.return_value = AIMessage(
            content="SEVERITY: critical\nINSIGHT: LeBron ties it at 89."
        )

        state = make_state(
            event={
                "actionNumber": 412,
                "description": "James Driving Layup",
                "playerName": "LeBron James",
            },
            game_context={
                "period": 4,
                "clock": "1:50",
                "score_home": 89,
                "score_away": 89,
                "home_team": "GSW",
                "away_team": "CLE",
            },
        )

        result = generate_insight(state)

        assert result["action"] == Action.ANALYZED
        assert result["severity"] == "critical"
        assert "LeBron" in result["insight"]

    @patch("src.agent._narrator")
    def test_emits_send_alert_tool_call(self, mock_narrator: MagicMock) -> None:
        # The graph relies on generate_insight returning an AIMessage with a
        # send_alert tool_call so the downstream ToolNode can persist.
        mock_narrator.invoke.return_value = AIMessage(
            content="SEVERITY: notable\nINSIGHT: A solid run by Cleveland."
        )
        state = make_state()

        result = generate_insight(state)

        msgs = result["messages"]
        assert len(msgs) == 1
        ai = msgs[0]
        assert isinstance(ai, AIMessage)
        assert ai.tool_calls
        tc = ai.tool_calls[0]
        assert tc["name"] == "send_alert"
        assert tc["args"]["severity"] == "notable"
        assert "Cleveland" in tc["args"]["insight"]
        assert tc.get("id"), "tool_call must have an id for ToolNode matching"

    @patch("src.agent._narrator")
    def test_passes_tool_results_to_narrator(
        self, mock_narrator: MagicMock
    ) -> None:
        # When the classifier gathered context via tools, generate_insight
        # should fold those tool results into the narrator's user prompt.
        mock_narrator.invoke.return_value = AIMessage(
            content="SEVERITY: notable\nINSIGHT: stub"
        )
        state = make_state(
            messages=[
                ToolMessage(
                    content='{"points": 27, "rebounds": 11}',
                    name="get_player_stats",
                    tool_call_id="t1",
                ),
            ]
        )

        generate_insight(state)

        # Inspect the user message that was passed to the narrator.
        sent_messages = mock_narrator.invoke.call_args[0][0]
        user_content = sent_messages[1].content
        assert "27" in user_content
        assert "get_player_stats" in user_content


# --- _process_event dedup --------------------------------------------------


class TestProcessEventDedup:
    """PlayByPlayV3 sometimes emits two events per actionNumber (e.g. turnover
    + steal). The graph should only be invoked once per (gameId, actionNumber),
    but the tracker should fold both halves so foul/scoring state stays right.
    """

    @patch("src.agent._graph")
    def test_duplicate_action_number_invokes_graph_once(
        self, mock_graph: MagicMock
    ) -> None:
        mock_graph.invoke.return_value = {
            "action": Action.SKIPPED_OTHER,
            "severity": None,
            "messages": [],
        }
        tracker = GameContextTracker()
        seen: set = set()

        offensive = make_event(
            actionNumber=734,
            personId=201939,
            playerName="Strus",
            description="Strus Turnover",
            actionType="Turnover",
        )
        defensive = make_event(
            actionNumber=734,
            personId=2544,
            playerName="Bridges",
            description="Bridges Steal",
            actionType="Steal",
        )

        _process_event(offensive, tracker, seen)
        _process_event(defensive, tracker, seen)

        assert mock_graph.invoke.call_count == 1
        # First call should have seen the offensive event.
        invoked_state = mock_graph.invoke.call_args_list[0][0][0]
        assert invoked_state["event"]["description"] == "Strus Turnover"

    @patch("src.agent._graph")
    def test_tracker_still_updates_on_duplicate(
        self, mock_graph: MagicMock
    ) -> None:
        # Even when the graph is deduped, both halves must flow through the
        # tracker so foul counts and scoring stay accurate.
        mock_graph.invoke.return_value = {
            "action": Action.SKIPPED_OTHER,
            "severity": None,
            "messages": [],
        }
        tracker = GameContextTracker()
        seen: set = set()

        # Two foul-adjacent events sharing an actionNumber but different
        # personIds — both fouls should be counted.
        first = make_event(
            actionNumber=200, personId=111, actionType="Foul"
        )
        second = make_event(
            actionNumber=200, personId=222, actionType="Foul"
        )

        _process_event(first, tracker, seen)
        _process_event(second, tracker, seen)

        assert mock_graph.invoke.call_count == 1
        assert tracker.player_fouls == {111: 1, 222: 1}

    @patch("src.agent._graph")
    def test_distinct_action_numbers_both_invoke_graph(
        self, mock_graph: MagicMock
    ) -> None:
        mock_graph.invoke.return_value = {
            "action": Action.SKIPPED_OTHER,
            "severity": None,
            "messages": [],
        }
        tracker = GameContextTracker()
        seen: set = set()

        _process_event(make_event(actionNumber=10), tracker, seen)
        _process_event(make_event(actionNumber=11), tracker, seen)

        assert mock_graph.invoke.call_count == 2
