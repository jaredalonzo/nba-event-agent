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
   Consumer + LangGraph agent
        ↓
   data/insights.jsonl
```

The consumer maintains a stateful `GameContextTracker` keyed by game ID — running score, quarter, time, last five scoring plays, per-player foul counts — folded across the event stream. Before each graph invocation, it attaches a fresh snapshot to the agent state. The graph itself is stateless across events.

### The agent loop

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
| LLM              | Claude Sonnet 4.6 (`claude-sonnet-4-6`)                 |
| Kafka client     | `confluent-kafka` (librdkafka)                          |
| Kafka runtime    | Confluent Platform 7.5 via Docker Compose               |
| NBA data         | `nba_api` (`PlayByPlayV3` endpoint)                     |
| Config           | `python-dotenv`                                         |
| Types            | `pydantic` v2                                           |

---

## Project layout

```
nba-event-agent/
├── docker-compose.yml      # Zookeeper + Kafka
├── requirements.txt
├── .env                    # ANTHROPIC_API_KEY, Kafka config
├── src/
│   ├── producer.py         # nba_api → Kafka
│   ├── agent.py            # LangGraph graph + consumer loop
│   ├── state.py            # AgentState TypedDict, Action enum
│   ├── tools.py            # get_player_stats, analyze_momentum, send_alert
│   └── output.py           # Insight persistence
├── data/
│   └── insights.jsonl      # Generated insights
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

# Configure environment (create .env with these vars)
cat > .env <<'EOF'
ANTHROPIC_API_KEY=sk-ant-...
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=nba.plays
KAFKA_GROUP_ID=nba-agent-group
NBA_GAME_ID=0041500407
PRODUCER_DELAY_SECONDS=0.5
EOF
```

### Run

Three terminals:

```bash
# Terminal 1: bring Kafka up
docker compose up -d

# Terminal 2: start the agent (consumer)
python -m src.agent

# Terminal 3: start the producer (in another terminal so you can watch both)
python -m src.producer
```

The producer streams 467 plays from Game 7 at a configurable delay (`PRODUCER_DELAY_SECONDS`, default 0.5s). The agent prints classification decisions in real time and writes notable insights to `data/insights.jsonl`.

### Re-running cleanly

The consumer uses `auto.offset.reset=earliest` with `enable.auto.commit=false`, but a stale consumer-group offset will still cause the second run to look like nothing happens. For a fresh replay, change `KAFKA_GROUP_ID` in `.env` or reset offsets:

```bash
docker exec nba-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group "$KAFKA_GROUP_ID" \
  --reset-offsets --to-earliest --topic nba.plays --execute
```

---

## Example output

```json
{
  "timestamp": "2026-05-20T20:14:11Z",
  "game_id": "0041500407",
  "period": 4,
  "clock": "1:50",
  "event": "LeBron James blocks Andre Iguodala's layup attempt",
  "severity": "critical",
  "insight": "LeBron's chase-down block on Iguodala with under two minutes to play is the kind of defensive play that changes a championship. Cleveland trails by zero — the score's tied at 89 — and James has now logged 27 points, 11 rebounds, 11 assists tonight."
}
```

Insights are appended line-by-line to `data/insights.jsonl` and mirrored to stdout.

---

## Roadmap

Beyond the current scope:

- Swap simulated play-by-play for a live WebSocket feed
- Add an MCP server tool (`get_player_profile`) to demonstrate MCP + LangGraph integration
- LangSmith tracing for observability
- FastAPI endpoint exposing the insight stream

---

## Repository

The full design doc, including the state schema, notability heuristics, and Kafka consumer semantics, is in [`CLAUDE.md`](./CLAUDE.md).
