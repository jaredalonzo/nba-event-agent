"""LLM cost + token tracking for the NBA agent.

Drop-in LangChain callback handler that records token usage across every
`ChatAnthropic.invoke` / `.ainvoke` call. Records four separately-billed
counters per response (matches Anthropic's API surface):

    input_tokens                  — fresh, uncached input
    cache_creation_input_tokens   — tokens written to the prompt cache
    cache_read_input_tokens       — tokens served from the prompt cache
    output_tokens                 — generated tokens

These are summed per-model at the end of a run, multiplied by per-token
prices, and written to ``data/runs.jsonl`` so consecutive runs (and future
optimizations) can be compared apples-to-apples.

Pricing constants are mid-2025 reference numbers — verify against the
Anthropic pricing page before quoting in any external context.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult


_DATE_SUFFIX_RE = re.compile(r"-\d{8}$")


def _strip_version_suffix(model_name: str) -> str:
    """Remove a trailing -YYYYMMDD date suffix from a model name.

    'claude-haiku-4-5-20251001' -> 'claude-haiku-4-5'
    'claude-sonnet-4-6'         -> 'claude-sonnet-4-6'  (no change)
    """
    return _DATE_SUFFIX_RE.sub("", model_name)

# Per-million-token pricing (USD). Sentinel values — update if Anthropic's
# pricing changes. Keyed by the model_name the API reports back, which is
# what shows up in response_metadata.model_name.
#
# Anthropic bills four counters at three different rates:
#   - input        (fresh prompt tokens)              — base rate
#   - cache_create (tokens written to ephemeral cache) — 1.25× base
#   - cache_read   (tokens served from cache)          — 0.1× base
#   - output       (generated tokens)                  — typically 5× base
_PRICING: dict[str, dict[str, float]] = {
    # Sonnet 4.x family
    "claude-sonnet-4-6": {
        "input": 3.00,
        "cache_create": 3.75,
        "cache_read": 0.30,
        "output": 15.00,
    },
    "claude-sonnet-4-5": {
        "input": 3.00,
        "cache_create": 3.75,
        "cache_read": 0.30,
        "output": 15.00,
    },
    # Haiku 4.5 — drop-in for the classifier in Cost M4
    "claude-haiku-4-5": {
        "input": 1.00,
        "cache_create": 1.25,
        "cache_read": 0.10,
        "output": 5.00,
    },
}


@dataclass
class _CallRecord:
    """One row per LLM response. Aggregated lazily in summary()."""

    model: str
    input_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    output_tokens: int


@dataclass
class CostTracker(BaseCallbackHandler):
    """LangChain callback handler that accumulates per-call token usage.

    Attach to a ChatAnthropic instance via ``callbacks=[tracker]`` (at
    construction or per-invoke). Survives across both sync and async calls.
    The handler is intentionally cheap on the hot path — every call just
    appends a small dataclass. All aggregation/math happens in summary().
    """

    calls: list[_CallRecord] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    # -- LangChain callback hook --------------------------------------------

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """Fired after every LLM completion. Pulls the usage block from
        ``response_metadata['usage']`` — the cleanest, most stable surface
        for Anthropic's four-counter token accounting."""
        for gen_list in response.generations:
            for gen in gen_list:
                msg = getattr(gen, "message", None)
                if msg is None:
                    continue
                meta = getattr(msg, "response_metadata", {}) or {}
                usage = meta.get("usage") or {}
                # Some chat-model callbacks come through without usage data
                # (e.g. mocked responses in tests). Skip them silently.
                if not usage:
                    continue
                self.calls.append(
                    _CallRecord(
                        model=meta.get("model_name") or meta.get("model") or "unknown",
                        input_tokens=int(usage.get("input_tokens") or 0),
                        cache_creation_input_tokens=int(
                            usage.get("cache_creation_input_tokens") or 0
                        ),
                        cache_read_input_tokens=int(
                            usage.get("cache_read_input_tokens") or 0
                        ),
                        output_tokens=int(usage.get("output_tokens") or 0),
                    )
                )

    # -- Aggregation --------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        """Roll up per-model totals + a grand total + cache hit rate.

        Returns a JSON-serializable dict suitable for printing or appending
        to a JSONL file. Includes a cost figure even for unpriced models —
        in that case ``cost_usd`` is None and a warning string is set so the
        caller can flag pricing-table drift.
        """
        per_model: dict[str, dict[str, Any]] = {}
        for r in self.calls:
            bucket = per_model.setdefault(
                r.model,
                {
                    "calls": 0,
                    "input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 0,
                },
            )
            bucket["calls"] += 1
            bucket["input_tokens"] += r.input_tokens
            bucket["cache_creation_input_tokens"] += r.cache_creation_input_tokens
            bucket["cache_read_input_tokens"] += r.cache_read_input_tokens
            bucket["output_tokens"] += r.output_tokens

        for model, b in per_model.items():
            # Anthropic's API returns versioned model names like
            # 'claude-haiku-4-5-20251001'. Our pricing table is keyed by
            # the unversioned alias ('claude-haiku-4-5') so prices stay
            # stable when new versions ship. Strip a trailing -YYYYMMDD
            # date suffix before looking up.
            lookup = _strip_version_suffix(model)
            prices = _PRICING.get(lookup)
            if prices is None:
                b["cost_usd"] = None
                b["warning"] = f"no pricing entry for model {model!r}"
                continue
            # Costs are reported per million tokens, so divide the raw token
            # totals by 1e6 before multiplying.
            b["cost_usd"] = round(
                (b["input_tokens"] * prices["input"]
                 + b["cache_creation_input_tokens"] * prices["cache_create"]
                 + b["cache_read_input_tokens"] * prices["cache_read"]
                 + b["output_tokens"] * prices["output"])
                / 1_000_000,
                6,
            )
            # Cache hit rate ignores fresh-and-uncached input tokens because
            # they weren't candidates for caching in the first place. The
            # ratio is across the cache-eligible portion: reads / (reads +
            # creations). If neither is set, the call wasn't a cache user.
            cached_total = (
                b["cache_read_input_tokens"] + b["cache_creation_input_tokens"]
            )
            b["cache_hit_rate"] = (
                round(b["cache_read_input_tokens"] / cached_total, 3)
                if cached_total > 0
                else None
            )

        total_cost = sum(
            (b["cost_usd"] or 0.0) for b in per_model.values()
        )
        return {
            "duration_seconds": round(time.time() - self.started_at, 1),
            "total_calls": len(self.calls),
            "total_cost_usd": round(total_cost, 6),
            "by_model": per_model,
        }

    # -- Persistence --------------------------------------------------------

    def append_to(self, path: Path, *, extra: dict[str, Any] | None = None) -> None:
        """Append one JSON line to ``path``. Creates the parent dir if missing.

        ``extra`` lets the caller tack on context (e.g. game_id, run_label)
        that the tracker itself doesn't know about. Useful for diffing
        baseline vs. optimized runs after the fact.
        """
        record = self.summary()
        if extra:
            record["context"] = extra
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    def format_summary(self) -> str:
        """Human-readable single-block summary, for stdout at shutdown."""
        s = self.summary()
        lines = [
            "",
            "─── cost summary ─────────────────────────────────────────",
            f"  duration: {s['duration_seconds']:.1f}s   "
            f"calls: {s['total_calls']}   "
            f"total: ${s['total_cost_usd']:.4f}",
        ]
        for model, b in s["by_model"].items():
            hit_rate = (
                f"{b['cache_hit_rate'] * 100:.0f}% cache hit"
                if b.get("cache_hit_rate") is not None
                else "no cache"
            )
            cost = (
                f"${b['cost_usd']:.4f}" if b["cost_usd"] is not None else "n/a"
            )
            lines.append(
                f"  {model:24s}  {b['calls']:>4} calls  "
                f"in={b['input_tokens']:>6}  "
                f"out={b['output_tokens']:>5}  "
                f"cr={b['cache_read_input_tokens']:>5}  "
                f"cc={b['cache_creation_input_tokens']:>5}  "
                f"{hit_rate:>15}  {cost:>10}"
            )
        lines.append(
            "──────────────────────────────────────────────────────────"
        )
        return "\n".join(lines)
