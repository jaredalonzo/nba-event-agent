# NBA Live Game Event Agent

## Project Overview

A LangGraph agent that consumes NBA play-by-play events from a Kafka topic and generates real-time narrative insights for notable moments. The agent uses tool-based reasoning to decide whether an event is worth analyzing, enriches it with player stats and momentum data, and emits a structured insight.

This project is a portfolio piece demonstrating event-driven agentic architecture using LangGraph, Kafka, and the Anthropic API.

---

## Architecture

```
Simulated Play-by-Play (nba_api) → Kafka Producer → Kafka Topic → LangGraph Agent → Insight Output
                                                                       │
                                                                       └─► MCP subprocess (career profile)
```

### Components

- **Producer — historical** (`producer.py`): Fetches a completed game's PBP from `nba_api.stats.endpoints.PlayByPlayV3`, publishes to Kafka with a configurable delay to simulate streaming.
- **Producer — live** (`producer_live.py`): Polls the NBA's live JSON CDN via `nba_live_client.py` and publishes new plays as they appear. Auto-discovers an in-progress game from today's scoreboard, or streams a specific `NBA_GAME_ID` if set.
- **Live client** (`nba_live_client.py`): Thin `requests`-based wrapper around `cdn.nba.com/static/json/liveData/{scoreboard,playbyplay}`. Bypasses `nba_api.live` (whose default headers are now blocked by NBA's CDN).
- **Consumer + Agent** (`agent.py`): Async Kafka consumer that feeds each event into the LangGraph agent. The agent reasons over the event and decides whether to act. Spawns the MCP subprocess at startup and holds the stdio session open for the run.
- **Tools** (`tools.py`): Local tools (`get_player_stats`, `analyze_momentum`, `send_alert`).
- **MCP server** (`mcp_server/server.py`): Separate stdio subprocess exposing one tool — `get_player_profile` — for career-level player context (bio, draft, school, previous teams, career averages, postseason career highs). Persistent disk cache at `data/player_profiles.json` so the per-player nba_api fetch happens once, ever.
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
│   ├── producer.py           # Historical: nba_api PlayByPlayV3 → Kafka (with delay)
│   ├── producer_live.py      # Live: cdn.nba.com poll → Kafka (as plays happen)
│   ├── nba_live_client.py    # Thin requests wrapper for the live CDN endpoints
│   ├── agent.py              # LangGraph graph + async Kafka consumer; spawns MCP server
│   ├── state.py              # AgentState TypedDict
│   ├── tools.py              # get_player_stats, analyze_momentum, send_alert
│   ├── output.py             # Insight logger
│   └── mcp_server/
│       └── server.py         # MCP server: get_player_profile (stdio subprocess)
├── data/
│   ├── insights.jsonl        # Persisted agent outputs (gitignored)
│   └── player_profiles.json  # MCP cache, populated on demand (gitignored)
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
| MCP | `mcp` (FastMCP server) + `langchain-mcp-adapters` (LangGraph bridge) |
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

### `get_player_profile(player_id: str) -> dict` *(MCP-backed)*
Career-level context: bio, draft (year/round/pick), school, current + previous teams (chronological, return-to-team order preserved), career averages (ppg/rpg/apg), postseason career highs. Lives in a separate stdio subprocess (`src/mcp_server/server.py`) bridged into the LangGraph agent via `langchain-mcp-adapters`. Persistent disk cache at `data/player_profiles.json`.

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
- **`call_tools`** — executes any of `get_player_stats`, `analyze_momentum`, or the MCP-bridged `get_player_profile`, appends results to `messages`, routes back to `classify_event`. The MCP tool is invoked async over the persistent stdio session opened in `main()`.
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

Two modes, selected by `KAFKA_REPLAY`:

**Default (resume mode)** — `KAFKA_REPLAY` unset:
- Stable `KAFKA_GROUP_ID` (no timestamp suffix).
- Agent commits the offset synchronously after each successfully processed event.
- Restart picks up exactly where it left off. This is what you want during live games or whenever you Ctrl-C and resume.
- First run on a fresh group still replays from offset 0 (via `auto.offset.reset=earliest`).

**Replay mode** — `KAFKA_REPLAY=true`:
- Group ID gets a timestamp suffix at runtime → every run gets a fresh group.
- Combined with `auto.offset.reset=earliest`, this replays the full topic from offset 0 every time.
- Use this for demos, cost benchmarking, or any run-to-run comparison where determinism matters more than picking up where you left off.

Other consumer config (constant across modes):
- `enable.auto.commit=false` — explicit commits only, so partial work never silently advances the offset.
- `max.poll.interval.ms=30m` — generous headroom for slow events (MCP nba_api fetches, occasional Anthropic retries). Even if we still get kicked out of the group, per-event commits mean we resume cleanly on restart.
- `session.timeout.ms=60s` — independent liveness probe.

Tools:
- `scripts/seek_offset.py inspect [--group ID]` — show topic state and any committed offsets for a named group.
- `scripts/seek_offset.py seed --group ID (--latest | --offset N | --minutes-ago N)` — commit a specific starting offset for the named group (skip ahead, jump back, etc.).

### Environment variable convention

`.env` supplies defaults; shell env vars override them. To override for one run, prefix the command:

```bash
KAFKA_GROUP_ID=foo NBA_GAME_ID=0042500304 .venv/bin/python -m src.agent
```

Entrypoints should use plain `load_dotenv()` — never `load_dotenv(override=True)`. The override flag silently clobbers shell env, which breaks the inline-prefix pattern above. If you're adding a new entrypoint, mirror the pattern.

---

## Producer Behavior

Two producers, same Kafka topic, agent doesn't care which one is upstream.

### Historical (`producer.py`)

- Fetches a specific completed game's PBP via `nba_api.stats.endpoints.playbyplayv3.PlayByPlayV3` (the older `PlayByPlay` and `PlayByPlayV2` endpoints are deprecated — stats.nba.com returns empty JSON for them)
- Recommended demo games: 2016 NBA Finals Game 7 (`0041500407`) or 2025 ECF Game 1 (`0042500301`) — both have high density of notable events
- Serializes each play as JSON, publishes to `nba.plays`
- Configurable artificial delay (`PRODUCER_DELAY_SECONDS`, default 0.5s) so the agent appears to see plays "live"

### Live (`producer_live.py`)

- Polls `cdn.nba.com/static/json/liveData/playbyplay/playbyplay_<gameId>.json` every `LIVE_POLL_SECONDS` (default 5s) via `src/nba_live_client.py`
- Bypasses `nba_api.live` because its default headers are now blocked by NBA's CDN (returns 403). Hits the JSON URLs directly with browser-like headers (UA + Origin + Referer)
- Dedups new actions by `actionNumber` set, publishes only new plays
- Two game-selection modes:
  - Explicit: `NBA_GAME_ID` set → stream that game (must currently be in progress)
  - Auto: `NBA_GAME_ID` empty → auto-discover the first in-progress game on today's slate from the scoreboard endpoint
- Injects two fields the live endpoint doesn't ship but the agent expects: `gameId` (on the parent in the live response) and `location` ("h"/"v", derived from `teamId` vs `homeTeam.teamId`)
- Exits cleanly on scoreboard `gameStatus=3` (game final) or SIGINT/SIGTERM

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

`.env` supplies defaults; shell env vars override them. To override for one
run, prefix the command: `KAFKA_GROUP_ID=foo .venv/bin/python -m src.agent`.

---

## Running the Project

```bash
# 1. Start Kafka
docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the agent (consumer) — also spawns the MCP server subprocess
python -m src.agent

# 4. In a separate terminal, run the producer
python -m src.producer
```

Note: `docker compose` (subcommand) is the modern Docker Desktop invocation. If
you have the legacy `docker-compose` binary installed, that works too.

---

## Future Extensions (Phase 2)

- ~~Swap simulated data for a live WebSocket feed~~ — done as polling against
  `cdn.nba.com/static/json/liveData/*`. The NBA's CDN doesn't offer push, but a
  5s poll gives ~10-30s end-to-end latency, which is fine for the demo. See
  `src/producer_live.py`.
- ~~Add an MCP server tool (`get_player_profile`) to demonstrate MCP + LangGraph integration~~ — done. See `src/mcp_server/server.py` and the MCP section in the README.
- Add a LangSmith tracing integration for observability
- Expose insights via a simple FastAPI endpoint

---

## TODOs (from initial design review)

Open items deferred from the first pass. Address before considering the project portfolio-ready.

- [x] **Flesh out the test plan.** ~~Tests are stubs.~~ Done — 96 unit tests
  across producer, tools, output, and agent helpers. A graph-level
  happy-path test (mocking both LLMs) is still missing as a nice-to-have.
- [x] **Replace `.env` with `.env.example` in the repo.** Done.
- [x] **Pull the MCP server tool forward from Phase 2.** Done — `get_player_profile` lives in `src/mcp_server/server.py` as a FastMCP stdio server, bridged into the agent via `langchain-mcp-adapters`. Caches to `data/player_profiles.json`. "Recent news" was dropped from the spec (no clean nba_api source); accolades likewise.
