"""LangGraph state schema for the NBA agent.

The state is what flows through the graph between nodes. A fresh state is
constructed per Kafka event; the consumer loop in ``agent.py`` builds it,
the graph mutates ``messages`` / ``action`` / ``insight`` as nodes run, and
the final value is logged and (in M5) persisted.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, TypedDict

from langgraph.graph.message import add_messages


class Action(str, Enum):
    """What the agent decided to do with a given play-by-play event.

    Values are strings (via ``str, Enum``) so they serialize cleanly to JSON
    when we log the action.
    """

    ANALYZED = "analyzed"                # Full pipeline ran, insight produced
    SKIPPED_EARLY_Q = "skipped_early_q"  # Low-stakes event in Q1–Q3
    SKIPPED_ROUTINE = "skipped_routine"  # Routine play (FT, sub, timeout)
    SKIPPED_OTHER = "skipped_other"      # Catch-all for future heuristics


class AgentState(TypedDict):
    """Per-event state passed through the LangGraph graph.

    Fields:
        event:        Raw play-by-play event from Kafka (one PlayByPlayV3 row).
        game_context: Snapshot from GameContextTracker at processing time.
        messages:     LangChain message history. Annotated with ``add_messages``
                      so node return values append rather than overwrite.
        action:       What the agent decided. Set by the graph as it runs.
        insight:      Final generated narrative, if any (None on skip).
        severity:     Severity assigned to the insight: routine / notable /
                      critical. None on skip paths.
    """

    event: dict
    game_context: dict
    messages: Annotated[list, add_messages]
    action: Action
    insight: str | None
    severity: str | None
    team_context: dict | None
