"""Kafka consumer + LangGraph agent for NBA play-by-play events.

Consumes events from the ``nba.plays`` topic, maintains running game context
via ``GameContextTracker``, and invokes a LangGraph agent that decides whether
to generate an insight or skip the event.

Filled in across Milestones 2-5.
"""
