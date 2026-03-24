from __future__ import annotations

from pathlib import Path

from spec2cov.config import FilterConfig

SV_EXTENSIONS = {".v", ".sv"}


def extension_allowed(path: str, allowed_extensions: list[str]) -> bool:
    return Path(path).suffix.lower() in {ext.lower() for ext in allowed_extensions}


def should_keep_sv_file(text: str, config: FilterConfig) -> tuple[bool, dict[str, int]]:
    lower_text = text.lower()
    hits = {keyword: lower_text.count(keyword.lower()) for keyword in config.sv_keywords}
    keep = (hits.get("module", 0) > 0 or hits.get("interface", 0) > 0) and sum(hits.values()) > 0
    return keep, hits


def has_minimum_text(text: str, min_chars: int) -> bool:
    return len(text.strip()) >= min_chars
