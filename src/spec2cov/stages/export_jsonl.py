from __future__ import annotations

import hashlib
from pathlib import Path

import orjson

from spec2cov.config import AppConfig
from spec2cov.db.repository import Database
from spec2cov.exporters import agentic, non_agentic
from spec2cov.logging_utils import get_logger


def _load_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8", errors="ignore")


def _artifact_to_context_key(artifact: dict) -> str:
    artifact_type = artifact["artifact_type"]
    suffix = Path(str(artifact["content_path"])).suffix or ".txt"
    if artifact_type == "spec":
        return f"docs/{artifact['artifact_name']}{suffix}"
    if artifact_type == "plan":
        return f"docs/{artifact['artifact_name']}{suffix}"
    if artifact_type == "dut":
        return f"rtl/{artifact['artifact_name']}{suffix}"
    if artifact_type == "cover":
        return f"verif/{artifact['artifact_name']}{suffix}"
    if artifact_type == "hvp":
        return f"verif/{artifact['artifact_name']}{suffix}"
    return f"misc/{artifact['artifact_name']}{suffix}"


def build_samples(db: Database, config: AppConfig) -> None:
    db.clear_samples()
    for repo in db.list_repositories():
        repo_id = int(repo["repo_id"])
        artifacts = db.list_artifacts(repo_id=repo_id)
        if not artifacts:
            continue
        grouped = {"spec": [], "plan": [], "dut": [], "cover": [], "hvp": []}
        for artifact in artifacts:
            grouped.setdefault(str(artifact["artifact_type"]), []).append(artifact)

        if not grouped["cover"] or not grouped["dut"]:
            continue

        input_context: dict[str, str] = {}
        output_context: dict[str, str] = {}
        for artifact_type in ("spec", "plan", "dut"):
            for artifact in grouped.get(artifact_type, []):
                input_context[_artifact_to_context_key(artifact)] = _load_text(str(artifact["content_path"]))
        for artifact_type in ("cover", "hvp"):
            for artifact in grouped.get(artifact_type, []):
                output_context[_artifact_to_context_key(artifact)] = _load_text(str(artifact["content_path"]))

        sample_key = str(repo["full_name"]).replace("/", "_")
        categories = [config.export.default_category, config.export.default_difficulty]
        db.upsert_sample(
            repo_id=repo_id,
            sample_key=sample_key,
            difficulty=config.export.default_difficulty,
            categories=categories,
            input_artifacts=input_context,
            output_artifacts=output_context,
            build_status="ready",
        )


def export_format(db: Database, config: AppConfig, run_id: int, format_name: str) -> Path:
    samples = db.list_samples(only_ready=True)
    builder = non_agentic.build_record if format_name == "non-agentic" else agentic.build_record
    output_path = config.export_dir / ("non_agentic.jsonl" if format_name == "non-agentic" else "agentic.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[bytes] = []
    ids: list[str] = []
    for sample in samples:
        record = builder(sample, config)
        ids.append(record["id"])
        lines.append(orjson.dumps(record))

    payload = b"\n".join(lines) + (b"\n" if lines else b"")
    output_path.write_bytes(payload)
    manifest = {
        "format": format_name,
        "record_ids": ids,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    db.record_export(format_name=format_name, output_path=str(output_path), run_id=run_id, record_count=len(lines), manifest=manifest)
    return output_path


def run(config: AppConfig, formats: list[str]) -> int:
    logger = get_logger(__name__)
    db = Database(str(config.db_path))
    run_id = db.create_pipeline_run(stage="export-jsonl", config_snapshot=config.snapshot(), code_version=config.code_version)
    try:
        build_samples(db, config)
        for format_name in formats:
            output_path = export_format(db, config, run_id=run_id, format_name=format_name)
            logger.info("export_jsonl.completed", run_id=run_id, format=format_name, output_path=str(output_path))
        db.finish_pipeline_run(run_id, status="completed")
        return run_id
    except Exception as exc:
        db.finish_pipeline_run(run_id, status="failed", error_summary=str(exc))
        raise
