"""Kafka consumer + LangGraph agent for NBA play-by-play events.

Filled out across milestones:
    M2: Consumer loop + GameContextTracker.
    M3 (current): Minimal LangGraph wired in — single classify_event node
                  that always routes to END with action=SKIPPED_OTHER.
                  Validates the plumbing before adding real classification.
    M4: Tools + agentic loop with conditional edges.
    M5: generate_insight node + persistence.
"""

from __future__ import annotations

import json
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from src.state import Action, AgentState

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


_M3_SYSTEM_PROMPT = """You are an event classifier for an NBA play-by-play stream.

For this milestone, your only job is to respond with the word 'skip' for every
event. Do not analyze, do not provide commentary, do not call any tools. Just
respond 'skip'. We are validating the graph plumbing; real classification
logic ships in a later milestone."""


def classify_event(state: AgentState) -> dict:
    """LLM call that decides whether to act on an event.

    M3 behavior: ask the model to say 'skip' for every event. Always returns
    Action.SKIPPED_OTHER. M4 will replace this with real notability heuristics
    and tool-calling.
    """
    event = state["event"]
    context = state["game_context"]

    user_msg = (
        f"Event #{event.get('actionNumber')}: {event.get('description')}\n"
        f"Action type: {event.get('actionType')}\n"
        f"Period: {context.get('period')}  Clock: {context.get('clock')}\n"
        f"Score: {context.get('home_team')} {context.get('score_home')} - "
        f"{context.get('score_away')} {context.get('away_team')}\n\n"
        f"What should we do?"
    )

    response = _llm.invoke(
        [
            SystemMessage(content=_M3_SYSTEM_PROMPT),
            HumanMessage(content=user_msg),
        ]
    )

    return {
        "messages": [response],
        "action": Action.SKIPPED_OTHER,
    }


def build_graph():
    """Compile the LangGraph for the NBA agent.

    M3 shape:  START → classify_event → END
    M4 will add a call_tools node and conditional edges back to classify_event.
    """
    g = StateGraph(AgentState)
    g.add_node("classify_event", classify_event)
    g.add_edge(START, "classify_event")
    g.add_edge("classify_event", END)
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
            snapshot = tracker.update(event)
            processed += 1

            # Invoke the graph. The model is called inside classify_event.
            # In M3 every event routes straight to END with SKIPPED_OTHER.
            initial_state: AgentState = {
                "event": event,
                "game_context": snapshot,
                "messages": [],
                "action": Action.SKIPPED_OTHER,
                "insight": None,
            }
            final_state = _graph.invoke(initial_state)
            action = final_state["action"]

            desc = event.get("description") or "(no description)"
            score_str = (
                f"{snapshot['home_team'] or 'HOME'} {snapshot['score_home']} - "
                f"{snapshot['score_away']} {snapshot['away_team'] or 'AWAY'}"
            )
            print(
                f"[#{event.get('actionNumber', '?'):>3} "
                f"Q{snapshot['period']} {snapshot['clock']:>5}]  "
                f"{score_str:<22}  {desc:<55}  → {action}",
                flush=True,
            )

            # Every 50 events, dump the top 3 foul counts for color.
            if processed % 50 == 0:
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


if __name__ == "__main__":
    main()
