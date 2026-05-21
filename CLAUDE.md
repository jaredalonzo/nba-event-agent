# NBA Live Game Event Agent

## Project Overview

A LangGraph agent that consumes NBA play-by-play events from a Kafka topic and generates real-time narrative insights for notable moments. The agent uses tool-based reasoning to decide whether an event is worth analyzing, enriches it with player stats and momentum data, and emits a structured insight.

This project is a portfolio piece demonstrating event-driven agentic architecture using LangGraph, Kafka, and the Anthropic API.

---

## Architecture

```
Simulated Play-by-Play (nba_api) → Kafka Producer → Kafka Topic → LangGraph Agent → Insight Output
```

### Components

- **Producer** (`producer.py`): Fetches historical play-by-play data from `nba_api` and publishes events to a Kafka topic, simulating a live stream with a configurable delay between events.
- **Consumer + Agent** (`agent.py`): Kafka consumer that feeds each event into the LangGraph agent. The agent reasons over the event and decides whether to act.
- **Tools** (`tools.py`): Four tools the agent can call.
- **State** (`state.py`): Typed state schema passed through the LangGraph graph.
- **Output** (`output.py`): Handles insight logging (console + JSON file).

---

## Project Structure

```
nba-agent/
├── CLAUDE.md
├── docker-compose.yml        # Kafka + Zookeeper
├── requirements.txt
├── .env.example              # template; copy to .env and fill in (gitignored)
├── src/
│   ├── producer.py           # Fetches nba_api data, publishes to Kafka
│   ├── agent.py              # LangGraph graph definition + Kafka consumer loop
│   ├── state.py              # AgentState TypedDict
│   ├── tools.py              # get_player_stats, analyze_momentum, generate_insight, send_alert
│   └── output.py             # Insight logger
├── data/
│   └── insights.jsonl        # Persisted agent outputs
└── tests/
    ├── test_tools.py
    └── test_agent.py
```

---

## Tech Stack

| Layer | Library |
|---|---|
| Agent framework | `langgraph`, `langchain-anthropic` |
| LLM | Claude Sonnet 4.6 (`claude-sonnet-4-6`) via Anthropic API |
| Kafka client | `confluent-kafka` |
| NBA data | `nba_api` |
| Kafka runtime | Docker Compose (Confluent images) |
| Config | `python-dotenv` |
| Types | `pydantic` v2 |

---

## Agent State Schema

```python
class Action(str, Enum):
    ANALYZED = "analyzed"               # Agent ran the full pipeline and produced an insight
    SKIPPED_EARLY_Q = "skipped_early_q" # Skipped: low-stakes event in Q1–Q3
    SKIPPED_ROUTINE = "skipped_routine" # Skipped: routine play (FT, sub, timeout)
    SKIPPED_OTHER = "skipped_other"     # Skipped: catch-all for future heuristics

class AgentState(TypedDict):
    event: dict                  # Raw play-by-play event from Kafka
    game_context: dict           # Running score, quarter, time remaining, recent run
    messages: list               # LangGraph message history
    action: Action               # Enum describing what the agent did with this event
    insight: str | None          # Final generated insight, if any
```

### Who maintains `game_context`

The consumer loop in `agent.py` owns `game_context` — not the graph. Plays arrive from Kafka in order; the consumer maintains a stateful `GameContextTracker` keyed by `game_id` that folds each incoming event into a running view (score, quarter, time remaining, last 5 scoring plays, per-player foul counts). Before invoking the graph, the consumer attaches the current snapshot to `AgentState.game_context`. This keeps the graph itself stateless across events: each invocation gets a fresh `event` + pre-computed `game_context`, and the graph never has to look backward.

---

## Tools

Tools are deterministic data-fetch or side-effect functions. Insight generation is **not** a tool — it lives as a dedicated graph node (`generate_insight`, see below) because it's a model call, not a deterministic operation.

### `get_player_stats(player_id: str, game_id: str) -> dict`
Fetches the player's current game stats (points, rebounds, assists, +/-) plus season averages using `nba_api`. Returns a summary dict.

### `analyze_momentum(game_id: str, current_period: int, current_time: str) -> dict`
Looks at the last 5 scoring plays to determine which team has momentum. Returns scoring run info (e.g., "LAL on a 9-2 run").

### `send_alert(insight: str, severity: str) -> None`
Logs the insight to stdout and appends it to `data/insights.jsonl`. Severity levels: `"routine"` | `"notable"` | `"critical"`.

---

## LangGraph Graph

This is an **agentic loop**, not a fixed pipeline. The model decides which tools (if any) to call and in what order. The graph loops between the model node and the tool node until the model returns a plain-text response, at which point control transfers to `generate_insight` (if the model gathered enough context) or directly to `END` (if it decided to skip).

```
        [START]
           ↓
   ┌──→ classify_event  ← model: skip, call tools, or finalize
   │      ↓        ↓             ↓
   │   tool_calls  plain text    plain text + intent="analyze"
   │      ↓        ↓             ↓
   └── call_tools  [END]         generate_insight (node, LLM call)
       (get_player_stats,           ↓
        analyze_momentum)         send_alert (tool)
                                    ↓
                                  [END]
```

### Nodes
- **`classify_event`** — model call. Receives the raw event, `game_context`, and any prior tool results. Decides among three actions: emit `tool_calls` to gather more data, return plain text to skip, or signal it's ready to generate an insight (e.g., via a structured "ready" sentinel or a final tool call to a no-op `mark_ready` tool).
- **`call_tools`** — executes `get_player_stats` and/or `analyze_momentum`, appends results to `messages`, routes back to `classify_event`.
- **`generate_insight`** — dedicated node (not a tool) that calls Claude Sonnet 4.6 with the accumulated context to produce a 2–3 sentence ESPN-style narrative. Sets `state.insight`.
- **`send_alert`** — tool node that logs and persists the insight.

### Conditional edges
- After `classify_event`:
  - Last message has `tool_calls` for data-fetch tools → `call_tools`
  - Last message signals "ready to analyze" → `generate_insight`
  - Plain text (skip) → `END`, set `state.action` accordingly
- After `call_tools` → always back to `classify_event` (the loop)
- After `generate_insight` → `send_alert` → `END`

---

## Kafka Setup

Use Docker Compose with Confluent's Kafka image. One topic: `nba.plays`.

```yaml
# docker-compose.yml outline
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
```

Topic config: 1 partition, replication factor 1 (local dev).

### Consumer offset & group semantics

For a replayable demo, the consumer should be configured so each run sees the full event stream from the beginning:

- `auto.offset.reset=earliest` — if no committed offset exists for the group, start from the first message rather than the tail.
- `enable.auto.commit=false` — disable automatic offset commits while iterating on the agent, so a crash doesn't silently skip events on the next run.
- Use a **fresh `KAFKA_GROUP_ID`** for each demo run (e.g., `nba-agent-group-${timestamp}`), or manually reset offsets with `kafka-consumer-groups --reset-offsets --to-earliest` between runs.

Without these, the second run of the agent will appear to do nothing — the group's committed offset is at the end of the topic, so there are no new events to consume.

---

## Producer Behavior

- Fetches play-by-play for a specific historical game via `nba_api.stats.endpoints.playbyplayv3.PlayByPlayV3` (the older `PlayByPlay` and `PlayByPlayV2` endpoints are deprecated — stats.nba.com returns empty JSON for them)
- Recommended game: 2016 NBA Finals Game 7 (GameID: `0041500407`) — high density of notable events
- Serializes each play as JSON, publishes to `nba.plays`
- Configurable delay between events (default: 0.5s) to simulate live stream
- Include a `simulated_timestamp` field so the agent knows event ordering

---

## Notability Heuristics (guide the model's system prompt)

Tell the model to flag events as notable if any of the following are true:
- A scoring play that extends or cuts a lead to ≤5 points in Q4
- A player reaches a stat milestone (20 pts, 10 reb, 10 ast in-game)
- Three consecutive scoring plays by the same team (momentum run)
- A foul on a player already at 5 fouls (foul trouble)
- Any event in the final 2 minutes of Q4 or OT

Routine free throws, non-scoring substitutions, and timeouts in early quarters should be skipped.

---

## Environment Variables

```
ANTHROPIC_API_KEY=
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=nba.plays
KAFKA_GROUP_ID=nba-agent-group
NBA_GAME_ID=0041500407
PRODUCER_DELAY_SECONDS=0.5
```

---

## Running the Project

```bash
# 1. Start Kafka
docker-compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the agent (consumer)
python -m src.agent

# 4. In a separate terminal, run the producer
python -m src.producer
```

---

## Future Extensions (Phase 2)

- Swap simulated data for a live WebSocket feed
- Add an MCP server tool (`get_player_profile`) to demonstrate MCP + LangGraph integration
- Add a LangSmith tracing integration for observability
- Expose insights via a simple FastAPI endpoint

---

## TODOs (from initial design review)

Open items deferred from the first pass. Address before considering the project portfolio-ready.

- [x] **Flesh out the test plan.** ~~Tests are stubs.~~ Done — 96 unit tests
  across producer, tools, output, and agent helpers. A graph-level
  happy-path test (mocking both LLMs) is still missing as a nice-to-have.
- [x] **Replace `.env` with `.env.example` in the repo.** Done.
- [ ] **Pull the MCP server tool forward from Phase 2.** An MCP-backed `get_player_profile` (career stats, bio, recent news) is a stronger portfolio differentiator than the other Phase 2 items. Consider promoting it into Phase 1 scope and demoting FastAPI/LangSmith.
