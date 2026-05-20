"""LangChain tools the agent can invoke.

Provides deterministic data-fetch tools (``get_player_stats``,
``analyze_momentum``) and the side-effect tool ``send_alert``. Insight
generation lives in ``agent.py`` as a graph node, not here, because it is a
model call rather than a deterministic operation.

Filled in during Milestone 4 (data-fetch tools) and Milestone 5 (send_alert).
"""
