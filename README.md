# NBA Live Game Event Agent

A LangGraph agent that consumes NBA play-by-play events from Kafka and emits real-time, ESPN-style narrative insights for notable moments.

The agent reasons over each play, decides whether it's worth analyzing, calls tools to enrich it with player stats and momentum data, and produces a 2–3 sentence broadcast-style insight. Routine plays are skipped.

---

## Why this project

Event-driven agentic systems are an underexplored corner of the LLM-application space. Most agent demos run one-shot off a chat prompt; production agents tend to live behind a queue, reacting to events they didn't ask for. This project builds that pattern end to end on a domain (live sports) where the boundary between "notable" and "noise" is concrete enough to evaluate.

Demo data is the 2016 NBA Finals Game 7 — a high-density game with enough decisive moments (LeBron's chase-down block, Kyrie's go-ahead three, lead changes in the final two minutes) to exercise every branch of the agent's decision logic.

---

## Architecture

```
Simulated play-by-play (nba_api)
        ↓
   Kafka producer
        ↓
   Kafka topic: nba.plays
        ↓
   Consumer + LangGraph agent ──► MCP subprocess (career profile)
        ↓               ↓          TeamContextProvider (grounding)
   data/insights.jsonl  PostgreSQL (plays + agent decisions)
```

The consumer maintains a stateful `GameContextTracker` keyed by game ID — running score, quarter, time, last five scoring plays, per-player foul counts — folded across the event stream. Before each graph invocation, it attaches a fresh snapshot to the agent state. The graph itself is stateless across events.

At startup the agent spawns a separate MCP subprocess that exposes one tool, `get_player_profile`, for career-level context. The classifier sees it alongside the local tools and decides on its own when to reach for it (see [MCP integration](#mcp-integration) below).

Before every `generate_insight` call, a `TeamContextProvider` fetches the current head coach, win-loss record, playoff seed, and active roster for both teams from `nba_api` and injects them into the narrator prompt as an authoritative grounding block — so the model never has to assert coach or roster facts from training-data priors (see [Reference grounding](#reference-grounding) below).

### The agent loop

```
        [START]
           ↓
   ┌──→ classify_event  ← model: skip, call tools, or finalize
   │      ↓        ↓             ↓
   │   tool_calls  plain text    plain text + intent="analyze"
   │      ↓        ↓             ↓
   └── call_tools  [END]         generate_insight (node, LLM call)
       (get_player_stats,         [grounding injected here]
        analyze_momentum,           ↓
        get_team_context,         send_alert (tool)
        get_player_profile)         ↓
                                  [END]
```

This is an agentic loop, not a fixed pipeline. The model decides which tools to call and in what order. It can gather player stats, look at the last five scoring plays for momentum context, or skip the play outright. When it has enough information, control transfers to a dedicated `generate_insight` node — a separate LLM call optimized for ESPN-voice narrative — and then to `send_alert` to persist the output.

Insight generation is a graph node, not a tool, because tools are reserved for deterministic data fetches and side effects. Wrapping a model call as a "tool" blurs that abstraction.

---

## How the agent decides what's notable

The classifier's system prompt encodes a small set of heuristics:

- A scoring play that extends or cuts a lead to ≤5 points in Q4
- A player hitting a stat milestone in-game (20 pts, 10 reb, 10 ast)
- Three consecutive scoring plays by the same team (a run)
- A foul on a player already at 5 fouls (foul trouble)
- Any event in the final two minutes of Q4 or OT

Routine free throws, substitutions, and early-quarter timeouts are skipped without an LLM tool call beyond classification.

---

## Tech stack

| Layer            | Tool                                                    |
| ---------------- | ------------------------------------------------------- |
| Agent framework  | `langgraph`, `langchain-anthropic`                      |
| LLM — classifier | Claude Haiku 4.5 (`claude-haiku-4-5`) — routes events, decides tool calls |
| LLM — narrator   | Claude Sonnet 4.6 (`claude-sonnet-4-6`) — generates ESPN-voice narrative |
| Kafka client     | `confluent-kafka` (librdkafka)                          |
| Kafka runtime    | Confluent Platform 7.5 via Docker Compose               |
| NBA data         | `nba_api` (`PlayByPlayV3` endpoint)                     |
| MCP              | `mcp` (FastMCP server) + `langchain-mcp-adapters` bridge |
| Database         | PostgreSQL 16 + `asyncpg`                               |
| Config           | `python-dotenv`                                         |
| Types            | `pydantic` v2                                           |

---

## Project layout

```
nba-event-agent/
├── docker-compose.yml      # Zookeeper + Kafka + PostgreSQL
├── requirements.txt
├── .env.example            # template; copy to .env (gitignored)
├── src/
│   ├── producer.py         # nba_api → Kafka (historical)
│   ├── producer_live.py    # cdn.nba.com → Kafka (live polling)
│   ├── nba_live_client.py  # requests wrapper for the live CDN
│   ├── prefilter.py        # deterministic pre-filter; skips ~40-50% of events before the LLM
│   ├── agent.py            # LangGraph graph + async consumer; spawns MCP server
│   ├── db.py               # asyncpg pool, schema bootstrap, upsert helpers
│   ├── state.py            # AgentState TypedDict, Action enum
│   ├── tools.py            # get_player_stats, analyze_momentum, get_team_context, send_alert
│   ├── team_context.py     # TeamContextProvider: coach/record/roster, 24h TTL cache
│   ├── output.py           # Insight persistence
│   ├── cost_log.py         # LangChain callback: tracks token usage + cost per model, appends to data/runs.jsonl
│   └── mcp_server/
│       └── server.py       # MCP: get_player_profile (career context)
├── data/                   # gitignored runtime files
│   ├── insights.jsonl      # one record per agent-generated insight
│   ├── player_profiles.json # MCP career-context cache (populated on demand, no TTL)
│   ├── team_context.json   # TeamContextProvider cache (24h TTL, keyed by tricode+date)
│   └── runs.jsonl          # per-run token counts, cost, cache hit rate, duration
└── tests/
```

The full design — agent state schema, tool contracts, Kafka consumer semantics — lives in [`CLAUDE.md`](./CLAUDE.md).

---

## Running locally

### Prerequisites

- Docker Desktop (for Kafka)
- Python 3.12
- An Anthropic API key

### Setup

```bash
# Clone and enter
git clone https://github.com/jaredalonzo/nba-event-agent.git
cd nba-event-agent

# Create venv and install
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure environment — copy the template and fill in your API key
cp .env.example .env
# then edit .env and set ANTHROPIC_API_KEY (the other defaults are fine)
```

### Run

Three terminals:

```bash
# Terminal 1: bring Kafka + PostgreSQL up
docker compose up -d

# Terminal 2: start the agent (consumer)
python -m src.agent

# Terminal 3: start a producer (historical OR live — see below)
python -m src.producer       # historical replay
# python -m src.producer_live  # live polling
```

The agent prints classification decisions in real time and writes notable insights to `data/insights.jsonl`.

#### Two producer modes

| | `src/producer.py` (historical) | `src/producer_live.py` (live) |
|---|---|---|
| **Data source** | `nba_api.stats.endpoints.PlayByPlayV3` | `cdn.nba.com/static/json/liveData/*` |
| **Game selection** | Set `NBA_GAME_ID` to any completed game | Priority order: (1) `NBA_GAME_ID` set → stream that specific game; (2) `NBA_TEAM` set → find today's game for that tricode (e.g. `NYK`); (3) auto-discover the first in-progress game on today's slate |
| **Pacing** | Configurable artificial delay (`PRODUCER_DELAY_SECONDS`) | Poll interval (`LIVE_POLL_SECONDS`, default 5s) — dictated by the game |
| **Duration** | A few minutes (467 events × 0.5s) | A few hours (real game length) |
| **Available when** | Any time | Only during the NBA season, when a game is actually on |

The historical producer is the demo path — works offline, reproducible. The live producer is the real-event-driven version — same agent, same Kafka, same insights pipeline; just a different source of truth.

### Re-running cleanly

By default the consumer commits offsets per-event, so restarting picks up where it left off. For a full replay from offset 0, set `KAFKA_REPLAY=true`:

```bash
KAFKA_REPLAY=true python -m src.agent
```

This appends a timestamp to the consumer group ID on each run, giving every run a fresh group. Combined with `auto.offset.reset=earliest`, the full topic replays from the beginning every time. Use this for demos, cost benchmarking, or any time you want deterministic reruns.

---

## Example output

```json
{
  "timestamp": "2026-05-20T20:14:11Z",
  "severity": "critical",
  "insight": "LeBron's chase-down block on Iguodala with under two minutes to play is the kind of defensive play that changes a championship. Cleveland trails by zero — the score's tied at 89 — and James has now logged 27 points, 11 rebounds, 11 assists tonight.",
  "event": {
    "gameId": "0041500407",
    "actionNumber": 412,
    "period": 4,
    "clock": "PT01M50.00S",
    "description": "LeBron James blocks Andre Iguodala's layup attempt",
    "playerName": "LeBron James",
    "scoreHome": "89",
    "scoreAway": "89"
  }
}
```

Insights are appended line-by-line to `data/insights.jsonl` and mirrored to stdout.

---

## MCP integration

The agent's career-context tool, `get_player_profile`, lives in a separate process speaking the [Model Context Protocol](https://modelcontextprotocol.io/) over stdio. The agent spawns it at startup and holds the session open for the run:

```python
mcp_client = MultiServerMCPClient({
    "nba": {
        "command": sys.executable,
        "args": ["-m", "src.mcp_server.server"],
        "transport": "stdio",
    }
})

async with mcp_client.session("nba") as session:
    mcp_tools = await load_mcp_tools(session)
    all_tools = AGENT_TOOLS + mcp_tools
    # bind to the classifier LLM, build the graph, run the consumer loop…
```

Two things worth flagging:

- **Why MCP for this tool specifically.** The local tools in `src/tools.py` operate on the current game — box-score stats, momentum runs, alerts. `get_player_profile` is a different shape: it pulls career-level data that doesn't change mid-game and is worth caching to disk. Moving it behind MCP keeps that long-lived data fetcher (and its cache) cleanly out of the per-event tool surface, and demonstrates the protocol-level integration as a portfolio piece.

- **Persistent session matters.** A re-spawn-per-call setup runs ~570ms per tool invocation (subprocess startup + handshake). Holding the session open via `client.session("nba")` drops that to ~1ms. The MCP subprocess maintains its own in-memory cache mirroring `data/player_profiles.json`, so repeat lookups for the same player are effectively free.

The MCP-bridged tool is async-only — `langchain-mcp-adapters` wraps it as a LangChain `StructuredTool` without sync support. That's why the agent's `main()` and `_process_event` are `async def` and the graph uses `ainvoke`; the Kafka consumer's blocking `poll` is dispatched to a thread executor so the event loop stays free.

---

## Observability

The agent ships with optional [LangSmith](https://smith.langchain.com) tracing. When enabled, every graph invocation — `classify_event → call_tools → generate_insight → send_alert` — is captured as a traced run, so you can inspect which plays were flagged, what tool calls were made, and exactly how each insight was generated.

### Setup

1. Sign up at [smith.langchain.com](https://smith.langchain.com) and create an API key.
2. In your `.env`, set:

```bash
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=<your-key>
LANGCHAIN_PROJECT=nba-agent   # groups runs under this project name
```

3. Run the agent as normal — traces appear in the LangSmith UI automatically.

### What gets tagged

Each run is tagged with `game_id`, `period`, and `action_number` metadata, so you can filter in the LangSmith UI to a specific game or quarter. For example, to find all Q4 events from a specific game:

```
metadata.game_id = "0041500407" AND metadata.period = 4
```

No code changes are needed to toggle tracing on or off — it's entirely controlled by the `LANGCHAIN_TRACING_V2` environment variable.

---

## Reference grounding

LLMs hallucinate facts they weren't trained on — or trained on outdated versions of. For sports, the most common symptom is a stale coach: the model confidently names last season's head coach because that's what it saw most often during training.

The fix is `TeamContextProvider` in `src/team_context.py`. Before every `generate_insight` call, it fetches the current head coach, win-loss record, playoff seed, and active roster for both teams from `nba_api` and injects them into the narrator's prompt as a grounding block:

```
Team context (authoritative):
Home: LAL (coach: JJ Redick, 53-29, seed 4)
Away: GSW (coach: Steve Kerr, 37-45, seed 10)
```

The narrator's system prompt instructs it to treat this block as ground truth and omit any coach or roster fact not present in it. Injection is non-optional — it happens on every analyze path regardless of what tools the classifier called.

**Two consumers, one provider.** The same `TeamContextProvider` singleton also backs a `get_team_context(team_tricode)` tool visible to the classifier. The injection path is the floor (always runs); the tool is the ceiling (classifier reaches for it when standings or roster depth would change its routing decision — e.g., a team in a tight playoff race).

**Cache design.** Results are cached to `data/team_context.json` with a 24h TTL, keyed by `(team_tricode, game_date)`. Career profiles (`player_profiles.json`) cache forever — coaches get fired, career stats don't. The key includes the game date so each day gets its own snapshot; cross-day bleed is impossible.

**Why local, not MCP.** `get_player_profile` sits behind MCP because it's permanent reference data cleanly separable from the per-event surface. Team context is the opposite: TTL-bound, session-current, and needed synchronously inside the graph before `generate_insight` runs. The rule: *permanent reference data is remote; session-current data is local.*

---

## Roadmap

Beyond the current scope:

- ~~Swap simulated play-by-play for a live WebSocket feed~~ — done as polling against `cdn.nba.com/static/json/liveData/*` (see `src/producer_live.py`)
- ~~Add an MCP server tool (`get_player_profile`) to demonstrate MCP + LangGraph integration~~ — done
- ~~Reference grounding for current-season team context (coach, record, roster)~~ — done (see Reference grounding section above)
- ~~LangSmith tracing for observability~~ — done (see Observability section above)
- FastAPI endpoint exposing the insight stream
- ~~PostgreSQL persistence for raw plays + agent decisions~~ — done (`src/db.py`, `asyncpg`, postgres service in Docker Compose)
- ~~Apache Flink as a stream processing layer~~ — evaluated and deferred; LLM latency is the bottleneck, not throughput. Worth revisiting at 10+ simultaneous games or if the project moves to the JVM.

---

## Repository

The full design doc, including the state schema, notability heuristics, and Kafka consumer semantics, is in [`CLAUDE.md`](./CLAUDE.md).
