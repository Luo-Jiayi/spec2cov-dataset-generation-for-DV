from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any

from spec2cov.config import AppConfig
from spec2cov.db.repository import Database, dumps
from spec2cov.logging_utils import get_logger
from spec2cov.parsing.doc_extractors import (
    build_dut_keyword_terms,
    build_spec_keyword_terms,
    extract_markdown_pdf_reference_lines,
    extract_hvp_text,
    extract_markdown_spec,
    extract_pdf_spec,
    extract_ralf_text,
    extract_terms,
    extract_xlsx_plan,
    extract_xml_plan,
    normalize_match_key,
)
from spec2cov.parsing.sv_pyverilog import extract_sv_cover_artifacts, extract_sv_dut_artifacts
from spec2cov.quality.gates import evaluate_repo_quality


ARTIFACT_SUFFIXES = {".xml": "plan", ".xlsx": "plan", ".hvp": "hvp", ".ralf": "cover", ".md": "spec", ".pdf": "spec", ".v": "sv", ".sv": "sv"}


def repo_slug(full_name: str) -> str:
    return full_name.replace("/", "__")


def artifact_filename(index: int, name: str, artifact_type: str) -> str:
    safe_name = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in name)[:80] or "artifact"
    return f"proj{index:04d}-{safe_name}-{artifact_type}"


def artifact_output_suffix(artifact: dict[str, Any]) -> str:
    metadata = artifact.get("metadata", {}) or {}
    source_type = str(metadata.get("source_type", "")).lower()
    if artifact["type"] in {"dut", "cover", "assert"} and source_type in {".v", ".sv"}:
        return ".sv"
    if artifact["type"] == "cover" and source_type == "ralf":
        return ".ralf"
    return ".txt"


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def build_artifact_row(repo_id: int, file_id: int | None, artifact: dict[str, Any], output_path: Path) -> dict[str, Any]:
    return {
        "repo_id": repo_id,
        "file_id": file_id,
        "artifact_type": artifact["type"],
        "artifact_name": output_path.stem,
        "content_path": str(output_path),
        "content_hash": content_hash(artifact["content"]),
        "char_count": len(artifact["content"]),
        "span_json": dumps(artifact.get("span", {})),
        "parser_json": dumps(artifact.get("parser", {})),
        "metadata_json": dumps(artifact.get("metadata", {})),
    }


def _basename_tokens(path: str, ignore_tokens: set[str]) -> set[str]:
    stem = Path(path).stem
    generic_tokens = {"plan", "spec"}
    parts = [normalize_match_key(part) for part in __import__("re").findall(r"[A-Za-z0-9]+", stem)]
    return {part for part in parts if part and part not in ignore_tokens and part not in generic_tokens}


def assign_project_indices(
    extracted: list[tuple[dict[str, Any], int | None]],
    repo_files: dict[str, dict[str, Any]],
    config: AppConfig,
) -> list[tuple[int, dict[str, Any], int | None]]:
    if not extracted:
        return []

    ignore_tokens = {normalize_match_key(token) for token in config.filters.project_cluster_ignore_name_tokens}
    file_count = len(repo_files)
    depth_tolerance = config.filters.project_cluster_path_depth_tolerance

    if file_count < config.filters.project_cluster_file_threshold:
        assigned: list[tuple[int, dict[str, Any], int | None]] = []
        for artifact, file_id in extracted:
            metadata = dict(artifact.get("metadata") or {})
            metadata["project_index"] = 1
            artifact["metadata"] = metadata
            assigned.append((1, artifact, file_id))
        return assigned

    def artifact_features(artifact: dict[str, Any]) -> tuple[set[str], set[str], str]:
        metadata = dict(artifact.get("metadata") or {})
        source_path = metadata.get("source_rel_path", "")
        artifact_terms = set(metadata.get("normalized_keywords") or []) | {normalize_match_key(value) for value in metadata.get("matched_terms", [])} | extract_terms(artifact["content"])
        basename_terms = _basename_tokens(source_path, ignore_tokens) if source_path else set()
        return basename_terms, artifact_terms, source_path

    clusters: list[dict[str, Any]] = []
    for artifact, _ in extracted:
        if artifact["type"] != "cover":
            continue
        basename_terms, artifact_terms, source_path = artifact_features(artifact)
        clusters.append({
            "basename_terms": set(basename_terms),
            "keyword_terms": set(artifact_terms),
            "paths": {source_path} if source_path else set(),
            "anchor_name": artifact.get("name", ""),
        })

    if not clusters:
        clusters = [{"basename_terms": set(), "keyword_terms": set(), "paths": set()}]

    assigned: list[tuple[int, dict[str, Any], int | None]] = []
    for artifact, file_id in extracted:
        metadata = dict(artifact.get("metadata") or {})
        basename_terms, artifact_terms, source_path = artifact_features(artifact)

        if artifact["type"] == "cover":
            artifact_name = artifact.get("name", "")
            for index, cluster in enumerate(clusters, start=1):
                if cluster.get("anchor_name") == artifact_name:
                    project_index = index
                    break
            else:
                project_index = 1
        else:
            best_indices: list[int] = []
            best_score = -1
            for index, cluster in enumerate(clusters, start=1):
                score = 0
                basename_overlap = bool(basename_terms and basename_terms & cluster["basename_terms"])
                keyword_overlap = bool(artifact_terms & cluster["keyword_terms"])
                if basename_overlap:
                    score += 1
                if keyword_overlap:
                    score += 4
                if not basename_overlap and not keyword_overlap:
                    score = -1
                if score > best_score:
                    best_score = score
                    best_indices = [index]
                elif score == best_score and score >= 0:
                    best_indices.append(index)
            target_indices = best_indices or [1]

            for project_index in target_indices:
                cluster = clusters[project_index - 1]
                cluster["basename_terms"].update(basename_terms)
                cluster["keyword_terms"].update(artifact_terms)
                if source_path:
                    cluster["paths"].add(source_path)

                assigned_metadata = dict(metadata)
                assigned_metadata["project_index"] = project_index
                assigned_artifact = {**artifact, "metadata": assigned_metadata}
                assigned.append((project_index, assigned_artifact, file_id))
            continue

        cluster = clusters[project_index - 1]
        cluster["basename_terms"].update(basename_terms)
        cluster["keyword_terms"].update(artifact_terms)
        if source_path:
            cluster["paths"].add(source_path)

        metadata["project_index"] = project_index
        artifact["metadata"] = metadata
        assigned.append((project_index, artifact, file_id))

    return assigned


def run(config: AppConfig, resume: bool = False) -> int:
    logger = get_logger(__name__)
    db = Database(str(config.db_path))
    run_id = db.create_pipeline_run(stage="preprocess", config_snapshot=config.snapshot(), code_version=config.code_version)

    try:
        existing_repo_ids = {artifact["repo_id"] for artifact in db.list_artifacts()}
        for repo in db.list_repositories():
            repo_id = int(repo["repo_id"])
            if resume and repo_id in existing_repo_ids:
                continue

            repo_dir = config.raw_dir / repo_slug(str(repo["full_name"]))
            if not repo_dir.exists():
                continue

            preprocess_dir = config.preprocess_dir / repo_slug(str(repo["full_name"]))
            if preprocess_dir.exists():
                shutil.rmtree(preprocess_dir)
            preprocess_dir.mkdir(parents=True, exist_ok=True)

            repo_files = {str(Path(row["path"])): row for row in db.list_candidate_files(repo_id=repo_id)}
            extracted: list[tuple[dict[str, Any], int | None]] = []
            md_pdf_reference_lines: list[str] = []
            same_file_dut_paths: set[str] = set()

            # Stage A: plan and hvp
            for raw_path in sorted(repo_dir.rglob("*")):
                if not raw_path.is_file():
                    continue
                rel_path = raw_path.relative_to(repo_dir).as_posix()
                file_row = repo_files.get(rel_path)
                file_id = int(file_row["file_id"]) if file_row else None
                suffix = raw_path.suffix.lower()
                if suffix == ".xml":
                    extracted.extend(({
                        **artifact,
                        "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                    }, file_id) for artifact in extract_xml_plan(raw_path))
                elif suffix == ".xlsx":
                    extracted.extend(({
                        **artifact,
                        "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                    }, file_id) for artifact in extract_xlsx_plan(raw_path))
                elif suffix == ".hvp":
                    extracted.extend(({
                        **artifact,
                        "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                    }, file_id) for artifact in extract_hvp_text(raw_path))

            # Stage B: cover + assert
            for raw_path in sorted(repo_dir.rglob("*")):
                if not raw_path.is_file():
                    continue
                rel_path = raw_path.relative_to(repo_dir).as_posix()
                file_row = repo_files.get(rel_path)
                file_id = int(file_row["file_id"]) if file_row else None
                suffix = raw_path.suffix.lower()
                if suffix == ".ralf":
                    extracted.extend(({
                        **artifact,
                        "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                    }, file_id) for artifact in extract_ralf_text(raw_path))
                elif suffix in {".v", ".sv"}:
                    for artifact in extract_sv_cover_artifacts(raw_path):
                        if artifact.get("metadata", {}).get("same_file_dut"):
                            same_file_dut_paths.add(rel_path)
                        extracted.append(
                            (
                                {
                                    **artifact,
                                    "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                                },
                                file_id,
                            )
                        )

            spec_terms = build_spec_keyword_terms([artifact for artifact, _ in extracted])
            dut_terms = build_dut_keyword_terms([artifact for artifact, _ in extracted])

            # Stage C: spec from md/pdf with cover-driven keywords
            for raw_path in sorted(repo_dir.rglob("*.md")):
                rel_path = raw_path.relative_to(repo_dir).as_posix()
                file_row = repo_files.get(rel_path)
                file_id = int(file_row["file_id"]) if file_row else None
                md_pdf_reference_lines.extend(extract_markdown_pdf_reference_lines(raw_path))
                extracted.extend(({
                    **artifact,
                    "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                }, file_id) for artifact in extract_markdown_spec(raw_path, spec_terms, config.filters.min_text_chars))

            for raw_path in sorted(repo_dir.rglob("*.pdf")):
                rel_path = raw_path.relative_to(repo_dir).as_posix()
                file_row = repo_files.get(rel_path)
                file_id = int(file_row["file_id"]) if file_row else None
                extracted.extend(({
                    **artifact,
                    "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                }, file_id) for artifact in extract_pdf_spec(raw_path, spec_terms, config.filters.min_text_chars))

            placeholder_created = False
            if md_pdf_reference_lines:
                extracted.append(
                    (
                        {
                            "type": "spec",
                            "name": "pdf-ref-placeholder",
                            "content": "\n".join(dict.fromkeys(md_pdf_reference_lines)),
                            "metadata": {
                                "source_type": "md_pdf_reference",
                                "placeholder": True,
                                "spec_short_exempt": True,
                                "source_rel_path": "",
                                "normalized_keywords": [],
                            },
                        },
                        None,
                    )
                )
                placeholder_created = True

            # Stage D: dut from sv driven by cover keywords
            for raw_path in sorted(repo_dir.rglob("*")):
                if not raw_path.is_file() or raw_path.suffix.lower() not in {".v", ".sv"}:
                    continue
                rel_path = raw_path.relative_to(repo_dir).as_posix()
                file_row = repo_files.get(rel_path)
                file_id = int(file_row["file_id"]) if file_row else None
                extracted.extend(({
                    **artifact,
                    "metadata": {**artifact.get("metadata", {}), "source_rel_path": rel_path},
                }, file_id) for artifact in extract_sv_dut_artifacts(raw_path, dut_terms, include_all=rel_path in same_file_dut_paths))

            clustered = assign_project_indices(extracted, repo_files, config)

            artifact_rows: list[dict[str, Any]] = []
            used_names: dict[str, int] = {}
            for _, (project_index, artifact, file_id) in enumerate(clustered, start=1):
                suffix = artifact_output_suffix(artifact)
                base_stem = artifact_filename(project_index, artifact["name"], artifact["type"])
                stem = Path(base_stem).stem
                count = used_names.get(stem, 0) + 1
                used_names[stem] = count
                final_filename = f"{stem}{suffix}" if count == 1 else f"{stem}-{count}{suffix}"
                output_path = preprocess_dir / final_filename
                output_path.write_text(artifact["content"], encoding="utf-8")
                artifact_rows.append(build_artifact_row(repo_id, file_id, artifact, output_path))

            # evaluate quality before committing this repo, log
            passed, score, counts, discard_reason = evaluate_repo_quality(
                artifact_rows,
                config.quality_gates,
                skip_spec_short_check=bool(md_pdf_reference_lines) and placeholder_created,
            )
            db.upsert_repo_quality(
                repo_id=repo_id,
                counts=counts,
                score=score,
                passed=passed,
                discard_reason=discard_reason,
                thresholds=config.quality_gates.model_dump(mode="json"),
            )
            if not passed:
                shutil.rmtree(preprocess_dir, ignore_errors=True)
                db.replace_artifacts_for_repo(repo_id, [])
                logger.info("preprocess.repo_discarded", run_id=run_id, repo=repo["full_name"], discard_reason=discard_reason)
                continue

            db.replace_artifacts_for_repo(repo_id, artifact_rows)
            logger.info("preprocess.repo_completed", run_id=run_id, repo=repo["full_name"], artifact_count=len(artifact_rows))

        db.finish_pipeline_run(run_id, status="completed")
        return run_id
    except Exception as exc:
        db.finish_pipeline_run(run_id, status="failed", error_summary=str(exc))
        raise
