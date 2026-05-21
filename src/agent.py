"""Kafka consumer + LangGraph agent for NBA play-by-play events.

Filled out across milestones:
    M2: Consumer loop + GameContextTracker.
    M3: Minimal LangGraph wired in (single classify_event node, always skip).
    M4:           Agentic loop with real notability classifier.
                  classify_event uses bound tools; ToolNode handles tool
                  execution and loops back. A finalize node parses the
                  classifier's final plain-text decision into an Action.
    M5 (current): generate_insight node + send_alert tool + persistence.
                  After classify_event returns ANALYZE, a separate LLM call
                  produces a 2–3 sentence ESPN-style narrative. The result is
                  emitted as a synthetic send_alert tool call, which the
                  PERSIST_TOOLS ToolNode runs to append the insight to
                  data/insights.jsonl.
"""

from __future__ import annotations

import json
import os
import signal
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from src.state import Action, AgentState
from src.tools import AGENT_TOOLS, PERSIST_TOOLS

# override=True so .env values trump pre-existing (empty) shell vars. Without
# this, an empty ANTHROPIC_API_KEY in the shell silently shadows the real key.
load_dotenv(override=True)

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
# Append a timestamp suffix so every run gets a fresh consumer group and replays
# the topic from the beginning. auto.offset.reset=earliest only kicks in when
# a group has no committed offset for the partition — so without this, a second
# run with the same group ID would silently see nothing (we're at the tail).
GROUP_ID = f"{os.environ['KAFKA_GROUP_ID']}-{int(time.time())}"


def parse_clock(pt: str | None) -> str:
    """Convert ISO-8601 duration ('PT12M00.00S') to 'MM:SS'."""
    if not pt or not pt.startswith("PT"):
        return pt or "00:00"
    body = pt[2:]
    minutes, _, rest = body.partition("M")
    seconds = rest.rstrip("S") or "0"
    return f"{int(minutes)}:{int(float(seconds)):02d}"


@dataclass
class GameContextTracker:
    """Stateful per-game snapshot, updated by folding plays in stream order.

    The tracker owns the running score, current quarter/clock, last 5 scoring
    plays (for momentum), and per-player foul counts. The LangGraph agent
    reads a fresh snapshot for each event and never has to look backward.
    """

    game_id: str | None = None
    period: int = 1
    clock: str = "12:00"
    score_home: int = 0
    score_away: int = 0
    home_team: str = ""
    away_team: str = ""
    last_scoring_plays: deque = field(default_factory=lambda: deque(maxlen=5))
    player_fouls: dict[int, int] = field(default_factory=dict)

    def update(self, event: dict) -> dict:
        """Fold one play into the running state, return a fresh snapshot."""
        if not self.game_id and event.get("gameId"):
            self.game_id = event["gameId"]

        if event.get("period"):
            self.period = int(event["period"])
        if event.get("clock"):
            self.clock = parse_clock(event["clock"])

        # location is "h" (home) or "v" (visitor) — capture team tricodes once.
        tricode = event.get("teamTricode") or ""
        location = event.get("location")
        if location == "h" and tricode and not self.home_team:
            self.home_team = tricode
        if location == "v" and tricode and not self.away_team:
            self.away_team = tricode

        # scoreHome/scoreAway are strings, empty until the first basket lands.
        # Treat any change as a scoring play and push onto the rolling window.
        new_home = self._as_int(event.get("scoreHome"))
        new_away = self._as_int(event.get("scoreAway"))
        scored = False
        if new_home is not None and new_home != self.score_home:
            self.score_home = new_home
            scored = True
        if new_away is not None and new_away != self.score_away:
            self.score_away = new_away
            scored = True
        if scored:
            self.last_scoring_plays.append(
                {
                    "actionNumber": event.get("actionNumber"),
                    "team": tricode,
                    "player": event.get("playerName"),
                    "description": event.get("description"),
                    "score_home": self.score_home,
                    "score_away": self.score_away,
                    "period": self.period,
                    "clock": self.clock,
                }
            )

        # Foul tracking — increment per personId on any event whose actionType
        # contains "foul" (covers Personal Foul, Shooting Foul, Off. Foul, etc.).
        action_type = (event.get("actionType") or "").lower()
        if "foul" in action_type:
            pid = event.get("personId")
            if pid:
                self.player_fouls[pid] = self.player_fouls.get(pid, 0) + 1

        return self.snapshot()

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def snapshot(self) -> dict:
        return {
            "game_id": self.game_id,
            "period": self.period,
            "clock": self.clock,
            "score_home": self.score_home,
            "score_away": self.score_away,
            "score_margin": self.score_home - self.score_away,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "last_scoring_plays": list(self.last_scoring_plays),
            "player_fouls": dict(self.player_fouls),
        }


# --- LangGraph -------------------------------------------------------------

# Model is module-level so we don't re-instantiate per event. The HTTP client
# inside ChatAnthropic pools connections internally.
_llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)
_llm_with_tools = _llm.bind_tools(AGENT_TOOLS)

# Separate, no-tools client for the narrative pass. A higher temperature gives
# the ESPN voice a bit of personality without going off the rails.
_narrator = ChatAnthropic(model="claude-sonnet-4-6", temperature=0.4)


CLASSIFIER_SYSTEM_PROMPT = """You are a real-time analyst for NBA play-by-play events.
For each event, decide whether it warrants a deeper insight or should be skipped.

NOTABLE events (worth an insight) include:
- A scoring play that creates or cuts a lead to 5 points or fewer in Q4
- A player approaching or hitting an in-game stat milestone (20 pts, 10 reb, or 10 ast)
- Three consecutive scoring plays by the same team (a momentum run)
- A foul on a player who already has 5 fouls (foul trouble)
- Any event in the final 2 minutes of Q4 or overtime

SKIP routine events that don't need narrative:
- Free throws in a non-close game
- Substitutions
- Timeouts in early quarters (Q1, Q2)
- Rebounds, missed shots, or jump balls with no game-state implication

You have two tools available:
- get_player_stats(player_id, game_id): the player's current box-score line
- analyze_momentum(): the last 5 scoring plays and the recent momentum picture

Use tools only when needed. For obviously routine events, skip without
calling any tools. For potentially notable events, gather just enough context
to decide.

When you've made your decision, respond with EXACTLY ONE LINE in this format:
- "ANALYZE: <one-sentence reason>" — the event warrants an insight
- "SKIP_ROUTINE: <reason>" — routine play (FT, sub, timeout)
- "SKIP_EARLY: <reason>" — low-stakes Q1–Q3 event
- "SKIP_OTHER: <reason>" — any other reason to skip

Do not produce any text after the decision line.
"""


def _build_user_message(event: dict, context: dict) -> str:
    """Format a per-event user prompt for the classifier."""
    return (
        f"Event #{event.get('actionNumber')}: {event.get('description')}\n"
        f"Action type: {event.get('actionType')}  "
        f"Sub-type: {event.get('subType') or '—'}\n"
        f"Player: {event.get('playerName') or '—'}  "
        f"personId: {event.get('personId') or '—'}\n"
        f"Period: {context.get('period')}  Clock: {context.get('clock')}\n"
        f"Score: {context.get('home_team')} {context.get('score_home')} - "
        f"{context.get('score_away')} {context.get('away_team')}\n"
        f"Game ID: {event.get('gameId')}\n\n"
        f"Decide whether this event is notable."
    )


def classify_event(state: AgentState) -> dict:
    """LLM call. May emit tool calls (loop continues) or a plain-text decision.

    On first entry the message history is empty and we seed it with system +
    user. On loop re-entry (after call_tools returned tool results) the prior
    messages are present in state["messages"] and we feed them all back to
    the model so it can incorporate the tool results.
    """
    event = state["event"]
    context = state["game_context"]
    prior = state.get("messages", [])

    if not prior:
        messages = [
            SystemMessage(content=CLASSIFIER_SYSTEM_PROMPT),
            HumanMessage(content=_build_user_message(event, context)),
        ]
    else:
        # Loop re-entry: replay the full history so the model sees tool results.
        # System prompt isn't re-sent — it's already in prior[0].
        messages = list(prior)

    response = _llm_with_tools.invoke(messages)
    # On first entry we also append the system + user that we constructed,
    # so subsequent passes through this node have the full history. The
    # `finalize` node owns setting `action`.
    new_messages = [*messages, response] if not prior else [response]
    return {"messages": new_messages}


_ACTION_PREFIXES = {
    "ANALYZE": Action.ANALYZED,
    "READY": Action.ANALYZED,  # accept either prefix for forgiveness
    "SKIP_ROUTINE": Action.SKIPPED_ROUTINE,
    "SKIPPED_ROUTINE": Action.SKIPPED_ROUTINE,
    "SKIP_EARLY": Action.SKIPPED_EARLY_Q,
    "SKIPPED_EARLY": Action.SKIPPED_EARLY_Q,
    "SKIP_OTHER": Action.SKIPPED_OTHER,
    "SKIPPED_OTHER": Action.SKIPPED_OTHER,
    "SKIP": Action.SKIPPED_OTHER,  # bare 'skip' from M3-style replies
}


def _parse_action_from_text(text: str) -> Action:
    """Map the classifier's plain-text decision to an Action."""
    if not text:
        return Action.SKIPPED_OTHER
    head = text.strip().upper()
    for prefix, action in _ACTION_PREFIXES.items():
        if head.startswith(prefix):
            return action
    return Action.SKIPPED_OTHER


def route_after_classify(state: AgentState) -> str:
    """Conditional edge: dispatch the classifier's last message.

    Three possible outcomes:
        - The message has tool_calls → loop back through call_tools.
        - The message's text starts with ANALYZE / READY → generate an insight.
        - Anything else (SKIP_* or unknown) → finalize and end.
    """
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
        return "call_tools"
    content = getattr(last, "content", "") or ""
    parsed = _parse_action_from_text(content)
    if parsed == Action.ANALYZED:
        return "generate_insight"
    return "finalize"


def finalize(state: AgentState) -> dict:
    """Skip-path terminal: parse the classifier's last decision into an Action.

    Only reached on SKIP_* outcomes. The analyze path bypasses this node and
    sets ``action = ANALYZED`` directly from ``generate_insight``.
    """
    last = state["messages"][-1]
    content = getattr(last, "content", "") or ""
    return {"action": _parse_action_from_text(content)}


# --- Insight generation ----------------------------------------------------

INSIGHT_SYSTEM_PROMPT = """You are an NBA broadcast analyst writing real-time insights.
Given the event, the game context, and any stats the classifier gathered, produce a
2–3 sentence ESPN-style narrative.

Voice guidance:
- Active, energetic, but not breathless.
- Reference specific numbers when they were fetched (points, momentum, fouls).
- Tie the moment to the game situation (lead, quarter, time remaining).
- No play-by-play recitation — interpret the moment.

Also assign a severity:
- "critical" — game-deciding moments, late-Q4 lead changes, OT, milestones reached.
- "notable" — momentum shifts, foul trouble for a star, sustained runs.
- "routine" — should rarely fire from this node; default to "notable" if unsure.

Respond in EXACTLY this format, no preamble:

SEVERITY: <critical|notable|routine>
INSIGHT: <2–3 sentence narrative>
"""


def _build_narrator_user_message(event: dict, context: dict, tool_summary: str) -> str:
    """Compact user-side prompt for the narrator. Includes any tool results."""
    home = context.get("home_team") or "HOME"
    away = context.get("away_team") or "AWAY"
    score_line = (
        f"{home} {context.get('score_home', 0)} - "
        f"{context.get('score_away', 0)} {away}"
    )
    base = (
        f"Event: {event.get('description')}\n"
        f"Player: {event.get('playerName') or '—'}\n"
        f"Period: Q{context.get('period')}  Clock: {context.get('clock')}\n"
        f"Score: {score_line}\n"
    )
    if tool_summary:
        base += f"\nGathered context:\n{tool_summary}\n"
    return base


def _summarize_tool_results(messages: list) -> str:
    """Flatten any ToolMessage contents from the classifier loop into a string."""
    from langchain_core.messages import ToolMessage

    chunks: list[str] = []
    for m in messages:
        if isinstance(m, ToolMessage):
            # ToolMessage.content can be str or list[dict]; coerce to str.
            content = m.content
            if isinstance(content, list):
                content = json.dumps(content)
            chunks.append(f"- {m.name}: {content}")
    return "\n".join(chunks)


_SEVERITY_LINE = "SEVERITY:"
_INSIGHT_LINE = "INSIGHT:"


def _parse_narrator_response(text: str) -> tuple[str, str]:
    """Pull SEVERITY and INSIGHT out of the narrator's reply.

    Tolerant of stray whitespace and the narrator wrapping the insight onto
    multiple lines (we collect everything after ``INSIGHT:``).
    """
    severity = "notable"
    insight_parts: list[str] = []
    collecting_insight = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.upper().startswith(_SEVERITY_LINE):
            severity = line.split(":", 1)[1].strip().lower() or "notable"
            collecting_insight = False
        elif line.upper().startswith(_INSIGHT_LINE):
            insight_parts.append(line.split(":", 1)[1].strip())
            collecting_insight = True
        elif collecting_insight and line:
            insight_parts.append(line)
    insight = " ".join(insight_parts).strip()
    if not insight:
        # Narrator didn't follow the format. Fall back to using whatever it
        # produced so we don't drop the work on the floor.
        insight = text.strip()
    return severity, insight


def generate_insight(state: AgentState) -> dict:
    """Second LLM pass: turn classifier context into an ESPN-style narrative.

    Emits a synthetic ``send_alert`` tool call so the downstream ToolNode
    persists the insight without us having to call ``log_insight`` directly.
    """
    event = state["event"]
    context = state["game_context"]
    tool_summary = _summarize_tool_results(state.get("messages", []))

    messages = [
        SystemMessage(content=INSIGHT_SYSTEM_PROMPT),
        HumanMessage(
            content=_build_narrator_user_message(event, context, tool_summary)
        ),
    ]
    response = _narrator.invoke(messages)
    severity, insight = _parse_narrator_response(response.content or "")

    # Build an AIMessage with a synthetic tool_call for send_alert. ToolNode
    # then runs the @tool, which writes the JSONL line. The id matters because
    # ToolNode matches the resulting ToolMessage back to this tool_call.
    persist_call = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "send_alert",
                "args": {"insight": insight, "severity": severity},
                "id": f"send_alert_{uuid.uuid4().hex[:8]}",
            }
        ],
    )

    return {
        "insight": insight,
        "severity": severity,
        "action": Action.ANALYZED,
        "messages": [persist_call],
    }


def build_graph():
    """Compile the LangGraph for the NBA agent.

    M5 shape::

                              ┌────────────────┐
                              ▼                │
            START → classify_event → call_tools
                      │
                      ├─ ANALYZE → generate_insight → send_alert (ToolNode) → END
                      │
                      └─ SKIP_*  → finalize → END
    """
    g = StateGraph(AgentState)
    g.add_node("classify_event", classify_event)
    g.add_node("call_tools", ToolNode(AGENT_TOOLS))
    g.add_node("generate_insight", generate_insight)
    g.add_node("send_alert", ToolNode(PERSIST_TOOLS))
    g.add_node("finalize", finalize)

    g.add_edge(START, "classify_event")
    g.add_conditional_edges(
        "classify_event",
        route_after_classify,
        {
            "call_tools": "call_tools",
            "generate_insight": "generate_insight",
            "finalize": "finalize",
        },
    )
    g.add_edge("call_tools", "classify_event")
    g.add_edge("generate_insight", "send_alert")
    g.add_edge("send_alert", END)
    g.add_edge("finalize", END)
    return g.compile()


# Compile once at import time. The graph is stateless across invocations.
_graph = build_graph()


# --- Kafka consumer --------------------------------------------------------


def build_consumer() -> Consumer:
    """Construct the Kafka consumer with replay-friendly defaults.

    auto.offset.reset=earliest: new groups read from offset 0.
    enable.auto.commit=false:   we don't want crashes to silently advance the
                                offset during iteration — explicit commits only.
    """
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


_running = True


def _handle_signal(signum, frame) -> None:
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(
        f"[agent] consuming {TOPIC} from {BOOTSTRAP_SERVERS} (group={GROUP_ID})",
        flush=True,
    )

    consumer = build_consumer()
    consumer.subscribe([TOPIC])

    tracker = GameContextTracker()
    # PlayByPlayV3 emits two events per (gameId, actionNumber) for plays with
    # both an offensive and defensive actor (turnover/steal, blocked shot, etc).
    # Tracker.update still folds both halves so foul counts and scoring stay
    # accurate, but the graph is invoked only for the first half so we don't
    # produce two contradictory insights for one moment.
    seen_pairs: set[tuple[str, int]] = set()
    processed = 0

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[agent] consumer error: {msg.error()}", flush=True)
                continue

            event = json.loads(msg.value())
            processed += 1
            _process_event(event, tracker, seen_pairs)

            # Every 50 events, dump the top 3 foul counts for color.
            if processed % 50 == 0:
                snapshot = tracker.snapshot()
                top_fouls = sorted(
                    snapshot["player_fouls"].items(), key=lambda kv: -kv[1]
                )[:3]
                if top_fouls:
                    print(
                        f"   ↳ top foulers (by personId): {top_fouls}", flush=True
                    )

    finally:
        consumer.close()
        print(f"\n[agent] consumed {processed} events. exiting.", flush=True)


def _process_event(
    event: dict,
    tracker: GameContextTracker,
    seen_pairs: set[tuple[str, int]],
) -> None:
    """Fold one event into the tracker, then invoke the graph unless deduped.

    Extracted so the dedup logic is testable without spinning up Kafka.
    """
    snapshot = tracker.update(event)

    pair_key = (event.get("gameId"), event.get("actionNumber"))
    if pair_key in seen_pairs:
        # Same (gameId, actionNumber) as a prior event — second half of a
        # paired play (e.g. turnover + steal). Tracker already updated above;
        # skip the graph invocation to avoid a second contradictory insight.
        print(
            f"[#{event.get('actionNumber', '?'):>3} "
            f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
            f"(dup actionNumber, graph skipped)  "
            f"{event.get('description') or '(no description)'}",
            flush=True,
        )
        return
    seen_pairs.add(pair_key)

    initial_state: AgentState = {
        "event": event,
        "game_context": snapshot,
        "messages": [],
        "action": Action.SKIPPED_OTHER,
        "insight": None,
        "severity": None,
    }
    final_state = _graph.invoke(initial_state)
    action = final_state["action"]
    severity = final_state.get("severity")

    tool_call_count = sum(
        len(getattr(m, "tool_calls", []) or [])
        for m in final_state.get("messages", [])
        if isinstance(m, AIMessage)
    )

    desc = event.get("description") or "(no description)"
    score_str = (
        f"{snapshot['home_team'] or 'HOME'} {snapshot['score_home']} - "
        f"{snapshot['score_away']} {snapshot['away_team'] or 'AWAY'}"
    )
    tool_hint = f"  [tools={tool_call_count}]" if tool_call_count else ""
    sev_hint = f"  [{severity}]" if severity else ""
    print(
        f"[#{event.get('actionNumber', '?'):>3} "
        f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
        f"{score_str:<22}  {desc:<55}  → {action}{sev_hint}{tool_hint}",
        flush=True,
    )


if __name__ == "__main__":
    main()
