"""Unit tests for src/output.py.

log_insight has one job: append a structured JSON record to insights.jsonl
and print a banner to stdout. We use tmp_path for the file destination so
tests don't touch the real data/ directory.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.output import _normalize_severity, log_insight


def _make_event(**overrides) -> dict:
    base = {
        "gameId": "0041500407",
        "actionNumber": 412,
        "period": 4,
        "clock": "PT01M50.00S",
        "description": "James Driving Layup",
        "playerName": "LeBron James",
        "scoreHome": "89",
        "scoreAway": "89",
    }
    base.update(overrides)
    return base


class TestNormalizeSeverity:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("critical", "critical"),
            ("CRITICAL", "critical"),
            (" Notable ", "notable"),
            ("routine", "routine"),
        ],
    )
    def test_known_buckets_pass_through(self, given: str, expected: str) -> None:
        assert _normalize_severity(given) == expected

    @pytest.mark.parametrize("given", ["weird", "", None, "important"])
    def test_unknown_defaults_to_notable(self, given) -> None:
        assert _normalize_severity(given) == "notable"


class TestLogInsight:
    def test_appends_jsonl_record(self, tmp_path: Path, capsys) -> None:
        target = tmp_path / "insights.jsonl"
        event = _make_event()
        insight = "LeBron just tied Game 7 at 89."

        record = log_insight(insight, "critical", event, path=target)

        # Returned record matches what was written.
        assert record["insight"] == insight
        assert record["severity"] == "critical"
        assert record["event"]["actionNumber"] == 412

        # File now has one JSON line.
        lines = target.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["insight"] == insight
        assert parsed["severity"] == "critical"
        assert parsed["event"]["playerName"] == "LeBron James"
        assert "timestamp" in parsed

    def test_appends_rather_than_overwrites(self, tmp_path: Path, capsys) -> None:
        target = tmp_path / "insights.jsonl"
        log_insight("first", "notable", _make_event(actionNumber=1), path=target)
        log_insight("second", "critical", _make_event(actionNumber=2), path=target)

        lines = target.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["insight"] == "first"
        assert json.loads(lines[1])["insight"] == "second"

    def test_creates_parent_directory(self, tmp_path: Path, capsys) -> None:
        # Target a path whose parent doesn't exist yet — log_insight should
        # mkdir it rather than crashing.
        target = tmp_path / "nested" / "deeper" / "insights.jsonl"
        log_insight("hi", "notable", _make_event(), path=target)
        assert target.exists()

    def test_unknown_severity_normalized_in_record(
        self, tmp_path: Path, capsys
    ) -> None:
        target = tmp_path / "insights.jsonl"
        log_insight("hi", "MEDIUM", _make_event(), path=target)
        record = json.loads(target.read_text().strip())
        assert record["severity"] == "notable"

    def test_prints_banner_to_stdout(self, tmp_path: Path, capsys) -> None:
        target = tmp_path / "insights.jsonl"
        log_insight(
            "Curry hits a corner three.",
            "notable",
            _make_event(description="Curry 27' 3PT"),
            path=target,
        )
        captured = capsys.readouterr()
        assert "Curry" in captured.out
        # Severity badge should appear too.
        assert "NOTABLE" in captured.out

    def test_event_projection_is_compact(self, tmp_path: Path, capsys) -> None:
        # We only persist a handful of event fields, not the whole row —
        # confirm we don't accidentally bloat the record.
        target = tmp_path / "insights.jsonl"
        bloated_event = _make_event(
            actionType="Made Shot",
            subType="Layup",
            personId=2544,
            location="v",
            # Stuff we explicitly do NOT want in the record:
            irrelevant_field="should not appear",
            another_field=12345,
        )
        log_insight("ok", "notable", bloated_event, path=target)
        record = json.loads(target.read_text().strip())
        assert set(record["event"].keys()) == {
            "gameId",
            "actionNumber",
            "period",
            "clock",
            "description",
            "playerName",
            "scoreHome",
            "scoreAway",
        }
