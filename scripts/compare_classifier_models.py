"""Compare Sonnet+cache vs Haiku on a realistic mix of NBA events,
measuring both cost and classification agreement.

Output: per-event decisions side-by-side + an agreement percentage,
followed by cost summaries for each model. Run before committing the
Haiku swap to validate the <10% disagreement threshold.
"""

from __future__ import annotations

import sys

from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

# shell env wins over .env — lets inline overrides take effect
load_dotenv()

sys.path.insert(0, ".")

from src.agent import (
    CLASSIFIER_SYSTEM_PROMPT,
    _build_user_message,
    _parse_action_from_text,
)
from src.cost_log import CostTracker
from src.tools import AGENT_TOOLS

# Realistic mix of NBA events — notable + routine across periods.
EVENTS = [
    # 1: Notable Q4 lead-change three (should be ANALYZE)
    (
        {
            "actionNumber": 401,
            "description": "LeBron James 25' 3-pointer (29 PTS)",
            "actionType": "Made Shot",
            "subType": "3pt",
            "playerName": "LeBron James",
            "personId": 2544,
            "gameId": "G",
        },
        {
            "period": 4,
            "clock": "1:24",
            "home_team": "LAL",
            "away_team": "BOS",
            "score_home": 101,
            "score_away": 99,
        },
    ),
    # 2: Routine Q1 free throw (skip)
    (
        {
            "actionNumber": 50,
            "description": "Brunson Free Throw 1 of 2",
            "actionType": "Free Throw",
            "playerName": "Jalen Brunson",
            "personId": 1628973,
            "gameId": "G",
        },
        {
            "period": 1,
            "clock": "6:10",
            "home_team": "NYK",
            "away_team": "IND",
            "score_home": 18,
            "score_away": 14,
        },
    ),
    # 3: Foul on close-game star (notable — might trigger tool call)
    (
        {
            "actionNumber": 320,
            "description": "Towns personal foul",
            "actionType": "Personal Foul",
            "playerName": "Karl-Anthony Towns",
            "personId": 1626157,
            "gameId": "G",
        },
        {
            "period": 3,
            "clock": "4:01",
            "home_team": "NYK",
            "away_team": "IND",
            "score_home": 71,
            "score_away": 70,
        },
    ),
    # 4: Substitution (skip routine)
    (
        {
            "actionNumber": 100,
            "description": "SUB: Rivers FOR Brunson",
            "actionType": "Substitution",
            "playerName": "Austin Rivers",
            "personId": 1,
            "gameId": "G",
        },
        {
            "period": 2,
            "clock": "7:40",
            "home_team": "NYK",
            "away_team": "IND",
            "score_home": 40,
            "score_away": 38,
        },
    ),
    # 5: Final-minute Q4 play (always notable)
    (
        {
            "actionNumber": 480,
            "description": "Jokic 12ft pull-up (33 PTS)",
            "actionType": "Made Shot",
            "subType": "2pt",
            "playerName": "Nikola Jokic",
            "personId": 203999,
            "gameId": "G",
        },
        {
            "period": 4,
            "clock": "0:28",
            "home_team": "DEN",
            "away_team": "MIN",
            "score_home": 95,
            "score_away": 93,
        },
    ),
    # 6: Q2 missed shot (skip)
    (
        {
            "actionNumber": 150,
            "description": "Edwards 18ft jumper MISS",
            "actionType": "Missed Shot",
            "playerName": "Anthony Edwards",
            "personId": 1,
            "gameId": "G",
        },
        {
            "period": 2,
            "clock": "5:20",
            "home_team": "DEN",
            "away_team": "MIN",
            "score_home": 48,
            "score_away": 46,
        },
    ),
    # 7: Momentum run play (notable)
    (
        {
            "actionNumber": 380,
            "description": "Curry 26' 3-pointer",
            "actionType": "Made Shot",
            "subType": "3pt",
            "playerName": "Stephen Curry",
            "personId": 201939,
            "gameId": "G",
        },
        {
            "period": 4,
            "clock": "8:42",
            "home_team": "GSW",
            "away_team": "MEM",
            "score_home": 84,
            "score_away": 76,
        },
    ),
    # 8: Q3 routine layup (likely skip)
    (
        {
            "actionNumber": 270,
            "description": "Adebayo cutting layup",
            "actionType": "Made Shot",
            "subType": "2pt",
            "playerName": "Bam Adebayo",
            "personId": 1,
            "gameId": "G",
        },
        {
            "period": 3,
            "clock": "9:01",
            "home_team": "MIA",
            "away_team": "BOS",
            "score_home": 58,
            "score_away": 55,
        },
    ),
]


def _decision_label(response) -> str:
    """Map a classifier response to a comparable decision string."""
    if getattr(response, "tool_calls", None):
        return "TOOL_CALL"
    text = response.content if isinstance(response.content, str) else (
        response.content[0].get("text", "") if response.content else ""
    )
    return _parse_action_from_text(text or "").value


def main() -> None:
    tr_sonnet = CostTracker()
    tr_haiku = CostTracker()
    sonnet = ChatAnthropic(
        model="claude-sonnet-4-6", temperature=0, callbacks=[tr_sonnet]
    ).bind_tools(AGENT_TOOLS)
    haiku = ChatAnthropic(
        model="claude-haiku-4-5", temperature=0, callbacks=[tr_haiku]
    ).bind_tools(AGENT_TOOLS)

    sys_msg = SystemMessage(
        content=[
            {
                "type": "text",
                "text": CLASSIFIER_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ]
    )

    decisions: list[tuple[str, str]] = []
    print(f"{'#':<5}{'event':<55}{'sonnet':<22}{'haiku':<22}")
    print("-" * 105)
    for i, (ev, ctx) in enumerate(EVENTS, 1):
        user = HumanMessage(content=_build_user_message(ev, ctx))
        r_s = sonnet.invoke([sys_msg, user])
        r_h = haiku.invoke([sys_msg, user])
        s = _decision_label(r_s)
        h = _decision_label(r_h)
        decisions.append((s, h))
        desc = ev["description"][:50]
        print(f"{i:<5}{desc:<55}{s:<22}{h:<22}")

    matches = sum(1 for s, h in decisions if s == h)
    n = len(decisions)
    print()
    print(f"Agreement: {matches}/{n} ({matches * 100 // n}%)")
    print()
    print("SONNET 4.6 (with caching):")
    print(tr_sonnet.format_summary())
    print()
    print("HAIKU 4.5 (no caching at this prompt size):")
    print(tr_haiku.format_summary())


if __name__ == "__main__":
    main()
