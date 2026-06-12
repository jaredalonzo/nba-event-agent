"""Kafka consumer + LangGraph agent for NBA play-by-play events."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from src import db as db_module
from src.cost_log import CostTracker
from src.prefilter import should_skip
from src.state import Action, AgentState
from src.tools import AGENT_TOOLS, PERSIST_TOOLS

# shell env wins over .env — lets inline overrides take effect
load_dotenv()

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
# Default behavior: stable group ID. The consumer commits after each event
# (see the loop), so a restart picks up exactly where it left off — useful
# during live games when the agent may get kicked from the group on a slow
# event, or when you Ctrl-C and want to resume without losing context.
#
# Set KAFKA_REPLAY=true to append a timestamp suffix to the group ID, which
# forces a fresh consumer group every run (and combined with
# auto.offset.reset=earliest, replays the topic from offset 0). Use this for
# demos, cost benchmarking, or any time you want deterministic re-runs.
_REPLAY = os.environ.get("KAFKA_REPLAY", "").strip().lower() in ("1", "true", "yes")
GROUP_ID = (
    f"{os.environ['KAFKA_GROUP_ID']}-{int(time.time())}"
    if _REPLAY
    else os.environ["KAFKA_GROUP_ID"]
)


def parse_clock(pt: str | None) -> str:
    """Convert ISO-8601 duration ('PT12M00.00S') to 'MM:SS'."""
    if not pt or not pt.startswith("PT"):
        return pt or "00:00"
    body = pt[2:]
    minutes, _, rest = body.partition("M")
    seconds = rest.rstrip("S") or "0"
    return f"{int(minutes)}:{round(float(seconds)):02d}"


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

# Classifier and narrator are intentionally on different models.
#
# Classifier: Claude Haiku 4.5 (~3x cheaper input, ~3x cheaper output than
# Sonnet 4.6). The classifier's job is routing — yes/no + which tool to
# call — and Haiku handles that reliably. A side-by-side eval on 8
# realistic events (scripts/compare_classifier_models.py) showed 87%
# decision agreement with Sonnet, with the single disagreement being a
# semantic equivalence (skipped_other vs skipped_early_q on an event
# both models correctly chose not to analyze).
#
# Why this also costs less than Sonnet-with-prompt-caching: classifier
# input is ~1572 tokens with tool schemas. Sonnet caches that prefix
# at ~$0.30/MTok on reads, but Haiku's flat $1/MTok input — combined
# with its much cheaper output ($5 vs $15/MTok) and a slight tendency
# to write tighter responses — beats Sonnet+cache by ~39% per call.
#
# The cache_control marker stays on the classifier system prompt
# (set in classify_event) even though Haiku silently drops it at this
# prompt size; it costs nothing and we get the savings automatically
# if Anthropic later lowers Haiku's cache minimum.
_llm = ChatAnthropic(model="claude-haiku-4-5", temperature=0)

# Stay safely under Tier 1's 50 RPM Haiku limit. classify_event is called
# once per event (or 2-3 times when it loops through tool calls), so 45 RPM
# gives ~10% headroom. Time-based bucket is sufficient because event
# processing is sequential — only one classify_event call is ever in flight.
_CLASSIFIER_RPM = int(os.environ.get("CLASSIFIER_RPM", "45"))
_classifier_min_interval = 60.0 / _CLASSIFIER_RPM
_classifier_last_call: float = 0.0

# Tool-bound classifier is built at startup (see main()), AFTER the MCP
# subprocess has been spawned and its tools discovered. We can't eagerly bind
# AGENT_TOOLS here because the agent would then be missing get_player_profile —
# the model wouldn't know it exists and would never call it.
_llm_with_tools: Any = None

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

You have three tools available:
- get_player_stats(player_id, game_id): the player's CURRENT-GAME box-score line
  (points, rebounds, assists, +/- in this game). Use this when you need to
  know how the player is performing right now.
- analyze_momentum(): the last 5 scoring plays and the recent momentum picture.
- get_player_profile(player_id): CAREER-LEVEL context — bio, draft position,
  seasons played, school, previous teams, career averages, postseason career
  highs. Use this sparingly, only when career context would meaningfully
  enrich the narrative: a player approaching a personal best, a veteran on
  a signature moment, a former #1 pick rising to the occasion, or a returning
  player playing against a previous team. Do NOT call it for routine notable
  plays where current-game stats are enough.

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
    global _classifier_last_call
    wait = _classifier_min_interval - (time.monotonic() - _classifier_last_call)
    if wait > 0:
        time.sleep(wait)
    _classifier_last_call = time.monotonic()

    event = state["event"]
    context = state["game_context"]
    prior = state.get("messages", [])

    if not prior:
        messages = [
            # Wrap the system prompt as a single cacheable content block.
            # The cache_control marker tells Anthropic to write the prompt
            # into the 5-minute ephemeral cache on first use; every later
            # event within that window (or the next classifier-loop call
            # for the same event) reads from cache at ~1/10th the input
            # rate. Note: Sonnet's minimum cacheable prefix is 1,024
            # tokens. CLASSIFIER_SYSTEM_PROMPT is at the borderline — the
            # tracker will report cache_create=0 if we slip below it.
            SystemMessage(
                content=[
                    {
                        "type": "text",
                        "text": CLASSIFIER_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ]
            ),
            HumanMessage(content=_build_user_message(event, context)),
        ]
    else:
        # Loop re-entry: replay the full history so the model sees tool results.
        # System prompt isn't re-sent here — it's already in prior[0] with its
        # cache_control marker preserved, so each loop pass reads from cache.
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

When the classifier fetched career-level context via get_player_profile
(seasons played, draft position, school, previous teams, postseason career
highs), weave ONE concrete detail into the narrative if and only if it
strengthens the moment — a milestone approach, a veteran rising for a
signature play, a player facing a former team, a young player crossing a
threshold for the first time. Do NOT shoehorn career context into routine
analysis. If nothing in the profile clearly elevates this specific moment,
don't reach for it.

Also assign a severity. Be disciplined here — if every play is "critical",
the label loses all meaning. Reserve the top bucket and default downward.

- "critical" — RARE. Only for plays that decide the game's outcome: a go-ahead
  or game-tying basket inside the final 30 seconds of a one-possession game,
  a buzzer-beater, an OT-winning shot, a player reaching a career or NBA
  record, an ejection of a star, or an injury that visibly changes the game.
  If you can imagine the game continuing normally after this play, it is NOT
  critical.
- "notable" — DEFAULT for any moment worth narrating: momentum runs, foul
  trouble for a star, lead changes outside the final minute, milestone
  approaches (e.g., player at 18 pts heading toward 20), big individual
  performances, key defensive stops mid-quarter. When in doubt, choose this.
- "routine" — for moments that, in hindsight, won't make the highlight reel
  — a made layup in a 12-point game, a non-clutch free throw, an early-quarter
  bucket that doesn't shift momentum. Use this when the classifier flagged
  the play but the gathered context shows it's less notable than it looked.

Target distribution across a typical game: roughly 10% critical, 60% notable,
30% routine. If you find yourself reaching for "critical" more than once or
twice per quarter, you're miscalibrated — step down to "notable".

Keep the narrative concise — 2-3 sentences is ideal.

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
        # Same caching pattern as classify_event. INSIGHT_SYSTEM_PROMPT is
        # comfortably above Sonnet's 1,024-token cache minimum, so this is
        # a near-certain hit on every event after the first within a 5-min
        # window. Critical for cost — the narrator runs on every analyzed
        # event and the prompt is the bulk of each call's input.
        SystemMessage(
            content=[
                {
                    "type": "text",
                    "text": INSIGHT_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        ),
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


def build_graph(data_tools: list[BaseTool] | None = None):
    """Compile the LangGraph for the NBA agent.

    The ``data_tools`` arg lets main() pass in AGENT_TOOLS + the MCP-bridged
    tools discovered at startup. Defaults to AGENT_TOOLS only so the function
    is still usable in tests and ad-hoc scripts that don't need MCP.

    M5 shape::

                              ┌────────────────┐
                              ▼                │
            START → classify_event → call_tools
                      │
                      ├─ ANALYZE → generate_insight → send_alert (ToolNode) → END
                      │
                      └─ SKIP_*  → finalize → END
    """
    tools = list(data_tools) if data_tools is not None else list(AGENT_TOOLS)
    g = StateGraph(AgentState)
    g.add_node("classify_event", classify_event)
    g.add_node("call_tools", ToolNode(tools))
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


# Compiled lazily in main() once MCP tools are available. Stays None until
# then; tests that need a graph build their own via build_graph() directly.
_graph: Any = None


# --- Kafka consumer --------------------------------------------------------


def build_consumer() -> Consumer:
    """Construct the Kafka consumer with resume-friendly defaults.

    auto.offset.reset=earliest: a brand-new group (first run, or KAFKA_REPLAY
                                mode) reads from offset 0. Once we've committed
                                an offset, this setting stops mattering.
    enable.auto.commit=false:   the consumer loop commits explicitly after each
                                successful event. Auto-commit would advance the
                                offset on a fixed timer regardless of whether
                                the event was actually processed, which can
                                silently skip events on a crash.
    max.poll.interval.ms=30m:   per-event processing can be slow (MCP nba_api
                                fetches on first-time players, occasional
                                Anthropic retry loops). The 5-min default kicks
                                the consumer out of the group on the long tail;
                                30 minutes gives generous headroom while still
                                catching a truly hung process. Even if we do
                                get kicked, per-event commits mean we resume
                                cleanly on restart.
    session.timeout.ms=60s:     liveness probe; independent of poll cadence.
    """
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 30 * 60 * 1000,
            "session.timeout.ms": 60 * 1000,
        }
    )


def _build_mcp_client() -> MultiServerMCPClient:
    """Construct the MCP client pointed at our local stdio server.

    Factored out so tests can mock it. The server is spawned as a Python
    subprocess running ``src.mcp_server.server`` over stdio — no port, no
    second terminal, no extra config to manage.
    """
    return MultiServerMCPClient(
        {
            "nba": {
                "command": sys.executable,
                "args": ["-m", "src.mcp_server.server"],
                "transport": "stdio",
            }
        }
    )


async def main() -> None:
    """Run the Kafka consumer + LangGraph agent.

    Async because the MCP-bridged tools require an event loop and don't
    support sync invocation. The consumer itself is a blocking C library
    (confluent_kafka), so each poll is dispatched to the default thread
    executor — the event loop stays unblocked and can service tool calls.

    Lifecycle:
        1. Open a persistent stdio session to the MCP subprocess
           (``async with client.session("nba")``). Re-spawning per call
           costs ~570ms each; a held session is ~1ms.
        2. Discover MCP tools and merge into the classifier's tool list.
        3. Build the graph with the combined toolset.
        4. Poll Kafka and process events until SIGINT/SIGTERM fires the
           stop event.
        5. Exit the ``async with`` to tear down the subprocess cleanly.
    """
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    # add_signal_handler is the async-safe way to wire SIGINT/SIGTERM. The
    # old `signal.signal` callback approach doesn't compose with the event
    # loop and can lose signals fired during awaits.
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler; fall back to the
            # classic API. Not a real concern for this project (mac/Linux only),
            # but keeps the agent importable on other platforms.
            signal.signal(sig, lambda *_: stop.set())

    print(
        f"[agent] consuming {TOPIC} from {BOOTSTRAP_SERVERS} (group={GROUP_ID})",
        flush=True,
    )

    consumer = build_consumer()

    _database_url = os.environ.get("DATABASE_URL", "").strip()
    db_pool: Any | None = None
    if _database_url:
        for attempt in range(15):  # 14 sleeps × 2s = 28s, covers postgres healthcheck worst case (25s)
            try:
                db_pool = await db_module.create_pool(_database_url)
                await db_module.ensure_schema(db_pool)
                print("[agent] postgres connected — plays and decisions will be persisted", flush=True)
                break
            except Exception as exc:
                if db_pool is not None:
                    await db_pool.close()
                    db_pool = None
                if attempt == 14:
                    print(f"[agent] postgres unavailable after 15 attempts, continuing without DB: {exc}", flush=True)
                else:
                    await asyncio.sleep(2)
    consumer.subscribe([TOPIC])

    # One tracker per run; attached as a callback on both ChatAnthropic
    # instances. The handler fires on every .invoke/.ainvoke and records
    # the four token counters separately, so the per-run summary can split
    # fresh vs. cached input cost.
    cost_tracker = CostTracker()
    _llm.callbacks = [cost_tracker]
    _narrator.callbacks = [cost_tracker]

    mcp_client = _build_mcp_client()
    global _llm_with_tools, _graph
    async with mcp_client.session("nba") as session:
        mcp_tools = await load_mcp_tools(session)
        print(
            f"[agent] MCP subprocess up — discovered {len(mcp_tools)} tool(s): "
            f"{[t.name for t in mcp_tools]}",
            flush=True,
        )
        all_data_tools = list(AGENT_TOOLS) + list(mcp_tools)
        _llm_with_tools = _llm.bind_tools(all_data_tools)
        _graph = build_graph(all_data_tools)

        tracker = GameContextTracker()
        # PlayByPlayV3 emits two events per (gameId, actionNumber) for plays
        # with both an offensive and defensive actor (turnover/steal, blocked
        # shot, etc). Tracker.update still folds both halves so foul counts
        # and scoring stay accurate, but the graph is invoked only for the
        # first half so we don't produce two contradictory insights for one
        # moment.
        seen_pairs: set[tuple[str, int]] = set()
        processed = 0

        try:
            while not stop.is_set():
                # poll() is a blocking C call; offload to the default
                # executor so the event loop stays free.
                msg = await loop.run_in_executor(None, consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[agent] consumer error: {msg.error()}", flush=True)
                    continue

                event = json.loads(msg.value())
                processed += 1
                await _process_event(event, tracker, seen_pairs, db_pool)

                # Commit synchronously after each successful event. This is
                # what makes restart-resume work: on the next run, the
                # consumer picks up at the next un-committed offset. The
                # commit is cheap (one round-trip to the broker) and the
                # event volume is low (a game produces ~500 events), so we
                # don't need to batch. If the agent crashes mid-event, the
                # event will be re-delivered on restart — duplicate work is
                # acceptable because insights.jsonl is append-only and a
                # human will see any duplicates immediately.
                consumer.commit(message=msg, asynchronous=False)

                # Every 50 events, dump the top 3 foul counts for color.
                if processed % 50 == 0:
                    snapshot = tracker.snapshot()
                    top_fouls = sorted(
                        snapshot["player_fouls"].items(), key=lambda kv: -kv[1]
                    )[:3]
                    if top_fouls:
                        print(
                            f"   ↳ top foulers (by personId): {top_fouls}",
                            flush=True,
                        )

        finally:
            consumer.close()
            if db_pool is not None:
                await db_pool.close()
            print(f"\n[agent] consumed {processed} events. exiting.", flush=True)
            # Always emit the cost summary, even on abnormal exit — the run
            # may have been short, but the tracker still has data worth
            # reviewing (especially while iterating on prompt changes).
            print(cost_tracker.format_summary(), flush=True)
            cost_tracker.append_to(
                Path("data") / "runs.jsonl",
                extra={"events_processed": processed, "group_id": GROUP_ID},
            )


async def _process_event(
    event: dict,
    tracker: GameContextTracker,
    seen_pairs: set[tuple[str, int]],
    db_pool: Any | None = None,
) -> None:
    """Fold one event into the tracker, then invoke the graph unless deduped.

    Extracted so the dedup logic is testable without spinning up Kafka.
    """
    snapshot = tracker.update(event)

    _game_id = event.get("gameId")
    _action_number = event.get("actionNumber")
    _db_active = db_pool is not None
    if _db_active and (_game_id is None or _action_number is None):
        print(
            f"[agent] WARNING: skipping DB write — missing gameId or actionNumber: {event}",
            flush=True,
        )
        _db_active = False

    if _db_active:
        await db_module.upsert_play(db_pool, event)

    pair_key = (_game_id, _action_number)
    if pair_key in seen_pairs:
        # Same (gameId, actionNumber) as a prior event — second half of a
        # paired play (e.g. turnover + steal). Tracker already updated above;
        # skip the graph invocation to avoid a second contradictory insight.
        print(
            f"[#{event.get('actionNumber') or '?':>3} "
            f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
            f"(dup actionNumber, graph skipped)  "
            f"{event.get('description') or '(no description)'}",
            flush=True,
        )
        return
    seen_pairs.add(pair_key)

    # Deterministic pre-filter: substitutions, period markers, early-quarter
    # timeouts, and blowout free throws in Q1-Q3 are mechanical skips. Catch
    # them here so we don't spend a classifier call on outcomes the prompt
    # would already route to SKIP_*. Anything ambiguous returns None and
    # falls through to the LLM.
    prefiltered = should_skip(event, snapshot)
    if prefiltered is not None:
        print(
            f"[#{event.get('actionNumber') or '?':>3} "
            f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
            f"(prefilter-skip)  "
            f"{event.get('description') or '(no description)'}  → {prefiltered}",
            flush=True,
        )
        return

    initial_state: AgentState = {
        "event": event,
        "game_context": snapshot,
        "messages": [],
        "action": Action.SKIPPED_OTHER,
        "insight": None,
        "severity": None,
    }
    final_state = await _graph.ainvoke(initial_state)
    action = final_state["action"]
    severity = final_state.get("severity")

    if _db_active:
        await db_module.persist_event_and_decision(
            db_pool,
            event,
            game_id=_game_id,
            action_number=_action_number,
            action=action.value,
            insight=final_state.get("insight"),
            severity=severity,
        )

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
        f"[#{event.get('actionNumber') or '?':>3} "
        f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
        f"{score_str:<22}  {desc:<55}  → {action}{sev_hint}{tool_hint}",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
