from __future__ import annotations

from collections import Counter
from typing import Iterable

from spec2cov.config import QualityGateConfig


def evaluate_repo_quality(
    artifact_rows: Iterable[dict],
    config: QualityGateConfig,
    skip_spec_short_check: bool = False,
) -> tuple[bool, float, dict[str, int], str | None]:
    counts = Counter(row["artifact_type"] for row in artifact_rows)
    total_artifacts = sum(counts.values())
    spec_chars = sum(row["char_count"] for row in artifact_rows if row["artifact_type"] == "spec")

    failures: list[str] = []
    if counts.get("cover", 0) < config.min_cover_blocks:
        failures.append("insufficient_cover_blocks")
    #if counts.get("plan", 0) < config.min_plan_segments and spec_chars < config.min_spec_chars and not skip_spec_short_check:
     #   failures.append("insufficient_plan_and_spec")
    if counts.get("dut", 0) < config.min_dut_blocks:
        failures.append("insufficient_dut_blocks")

    score = float(total_artifacts + counts.get("cover", 0) + counts.get("dut", 0))
    passed = not failures
    return passed, score, dict(counts), ",".join(failures) if failures else None
