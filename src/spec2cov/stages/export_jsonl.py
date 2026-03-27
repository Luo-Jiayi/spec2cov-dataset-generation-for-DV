from __future__ import annotations

import hashlib
import re
from pathlib import Path

import orjson
import yaml

from spec2cov.config import AppConfig
from spec2cov.db.repository import Database
from spec2cov.exporters import agentic, non_agentic
from spec2cov.logging_utils import get_logger


_ARTIFACT_PATTERN = re.compile(r"^(proj\d+)-(.+)-(spec|plan|dut|cover|assert|hvp)(\.[^.]+)$")
_PROMPT_FILE_NAME = "prompt.yaml"


def _load_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8", errors="ignore")


def _path_to_context_key(path: Path, artifact_type: str) -> str:
    if artifact_type in {"spec", "plan"}:
        return f"docs/{path.name}"
    if artifact_type == "dut":
        return f"rtl/{path.name}"
    if artifact_type in {"cover", "assert", "hvp"}:
        return f"verif/{path.name}"
    return f"misc/{path.name}"


def _load_prompts(config: AppConfig) -> tuple[str, str]:
    prompt_path = config.data_root / "codex-preprocess" / _PROMPT_FILE_NAME
    if not prompt_path.exists():
        return config.export.prompt_template, config.export.system_message
    raw = yaml.safe_load(prompt_path.read_text(encoding="utf-8")) or {}
    return str(raw.get("prompt_template", config.export.prompt_template)), str(raw.get("system_message", config.export.system_message))


def build_samples_from_codex_preprocess(config: AppConfig) -> list[dict]:
    base_dir = config.data_root / "codex-preprocess"
    samples: list[dict] = []
    if not base_dir.exists():
        return samples
    sample_id = 1
    for repo_dir in sorted((path for path in base_dir.iterdir() if path.is_dir()), key=lambda item: item.name.lower()):
        projects: dict[str, dict[str, list[Path]]] = {}
        for file_path in sorted((path for path in repo_dir.iterdir() if path.is_file()), key=lambda item: item.name.lower()):
            matched = _ARTIFACT_PATTERN.match(file_path.name)
            if not matched:
                continue
            project_id = matched.group(1)
            artifact_type = matched.group(3)
            project_group = projects.setdefault(project_id, {"spec": [], "plan": [], "dut": [], "cover": [], "assert": [], "hvp": []})
            project_group[artifact_type].append(file_path)

        for project_id in sorted(projects.keys()):
            grouped = projects[project_id]
            if not grouped["cover"] or not grouped["dut"]:
                continue

            input_context: dict[str, str] = {}
            output_context: dict[str, str] = {}
            for artifact_type in ("spec", "plan", "dut"):
                for file_path in grouped[artifact_type]:
                    input_context[_path_to_context_key(file_path, artifact_type)] = _load_text(file_path)
            for artifact_type in ("cover", "assert", "hvp"):
                for file_path in grouped[artifact_type]:
                    output_context[_path_to_context_key(file_path, artifact_type)] = _load_text(file_path)

            categories = [config.export.default_category, config.export.default_difficulty]
            sample_key = f"{repo_dir.name}_{project_id}"
            samples.append(
                {
                    "sample_id": sample_id,
                    "sample_key": sample_key,
                    "categories_json": orjson.dumps(categories).decode(),
                    "input_artifacts_json": orjson.dumps(input_context).decode(),
                    "output_artifacts_json": orjson.dumps(output_context).decode(),
                }
            )
            sample_id += 1
    return samples


def export_format(db: Database, config: AppConfig, run_id: int, format_name: str, samples: list[dict]) -> Path:
    prompt_template, system_message = _load_prompts(config)
    output_path = config.export_dir / ("non_agentic.jsonl" if format_name == "non-agentic" else "agentic.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[bytes] = []
    ids: list[str] = []
    for sample in samples:
        if format_name == "non-agentic":
            record = non_agentic.build_record(sample, config, prompt_template=prompt_template)
        else:
            record = agentic.build_record(sample, config, prompt_template=prompt_template, system_message=system_message)
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
        samples = build_samples_from_codex_preprocess(config)
        for format_name in formats:
            output_path = export_format(db, config, run_id=run_id, format_name=format_name, samples=samples)
            logger.info("export_jsonl.completed", run_id=run_id, format=format_name, output_path=str(output_path))
        db.finish_pipeline_run(run_id, status="completed")
        return run_id
    except Exception as exc:
        db.finish_pipeline_run(run_id, status="failed", error_summary=str(exc))
        raise
