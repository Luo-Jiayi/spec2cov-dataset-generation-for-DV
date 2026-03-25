from __future__ import annotations

import re
from pathlib import Path

from spec2cov.config import AppConfig
from spec2cov.db.repository import Database
from spec2cov.logging_utils import get_logger

MULTI_BLANK_RE = re.compile(r"(?:\n[ \t]*){2,}")


def _collapse_excess_blank_lines(text: str) -> str:
    normalized = text.replace("\r\n", "\n")
    collapsed = MULTI_BLANK_RE.sub("\n", normalized)
    return collapsed


def _iter_preprocess_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.txt") if path.is_file())


def run(config: AppConfig) -> int:
    logger = get_logger(__name__)
    db = Database(str(config.db_path))
    run_id = db.create_pipeline_run(stage="gen-retrieve", config_snapshot=config.snapshot(), code_version=config.code_version)
    try:
        changed_files = 0
        for path in _iter_preprocess_files(config.preprocess_dir):
            original = path.read_text(encoding="utf-8", errors="ignore")
            cleaned = _collapse_excess_blank_lines(original)
            if cleaned != original:
                path.write_text(cleaned, encoding="utf-8")
                changed_files += 1
        logger.info("gen_retrieve.completed", run_id=run_id, changed_files=changed_files, preprocess_dir=str(config.preprocess_dir))
        db.finish_pipeline_run(run_id, status="completed")
        return run_id
    except Exception as exc:
        db.finish_pipeline_run(run_id, status="failed", error_summary=str(exc))
        raise
