from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
from typing import Any

from spec2cov.config import AppConfig
from spec2cov.db.repository import Database
from spec2cov.filtering.dedup import compare_against_existing
from spec2cov.filtering.keyword_filter import SV_EXTENSIONS, extension_allowed, has_minimum_text, should_keep_sv_file
from spec2cov.logging_utils import get_logger
from spec2cov.sources.github_discovery import GitHubDiscovery
from spec2cov.sources.github_fetcher import GitHubFetcher


def repo_slug(full_name: str) -> str:
    return full_name.replace("/", "__")


def safe_repo_path(root: Path, full_name: str, relative_path: str) -> Path:
    normalized = PurePosixPath(relative_path)
    if normalized.is_absolute() or ".." in normalized.parts:
        raise ValueError(f"unsafe path: {relative_path}")
    return root / repo_slug(full_name) / Path(*normalized.parts)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def load_existing_dedup_records(raw_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not raw_dir.exists():
        return records
    for file_path in raw_dir.rglob("*"):
        if not file_path.is_file() or file_path.suffix.lower() == ".pdf":
            continue
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        digest = sha256_text(text)
        records.append({"text": text, "content_sha256": digest, "cluster_id": digest[:12]})
    return records


def repo_matches_policy(full_name: str, metadata: dict[str, Any], readme_text: str, config: AppConfig) -> tuple[bool, bool]:
    language = str(metadata.get("language") or "")
    allowed_languages = {value.lower() for value in config.discovery.repo_languages}
    language_ok = language.lower() in allowed_languages if language else False
    repo_name = full_name.rsplit("/", 1)[-1].lower()
    excluded_name_hit = any(keyword.lower() in repo_name for keyword in config.discovery.excluded_repo_name_keywords)
    haystacks = [full_name, str(metadata.get("description") or ""), readme_text]
    uvm_hit = any(keyword.lower() in haystack.lower() for haystack in haystacks for keyword in config.discovery.uvm_keywords)
    return language_ok and uvm_hit and not excluded_name_hit, uvm_hit


def merge_repo_candidates(search_candidates: list[dict[str, Any]], csv_candidates: list[dict[str, Any]], existing_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for candidate in [*search_candidates, *csv_candidates]:
        key = str(candidate["full_name"]).lower()
        canonical = existing_map.get(key, {}).get("full_name") or candidate["full_name"]
        if key not in merged:
            merged[key] = {**candidate, "full_name": canonical}
            continue
        existing = merged[key]
        sources = [existing.get("discovery_source"), candidate.get("discovery_source")]
        existing["discovery_source"] = ",".join(sorted({source for source in sources if source}))
        existing_seed = dict(existing.get("seed_metadata") or {})
        existing_seed.update(candidate.get("seed_metadata") or {})
        existing["seed_metadata"] = existing_seed
        if not existing.get("default_branch") and candidate.get("default_branch"):
            existing["default_branch"] = candidate["default_branch"]
    return list(merged.values())


def run(
    config: AppConfig,
    resume: bool = False,
    max_repos: int | None = None,
    max_files_per_repo: int | None = None,
    repo_csv: Path | None = None,
) -> int:
    logger = get_logger(__name__)
    db = Database(str(config.db_path))
    run_id = db.create_pipeline_run(stage="fetch-filter", config_snapshot=config.snapshot(), code_version=config.code_version)
    fetcher = GitHubFetcher(config)
    discovery = GitHubDiscovery(config, fetcher)
    existing_dedup_records = load_existing_dedup_records(config.raw_dir)

    try:
        existing_repo_map = db.list_repository_name_map()
        discovered = discovery.discover(max_repos=max_repos)
        csv_candidates = discovery.load_repo_candidates_from_csv(repo_csv) if repo_csv else []
        repo_candidates = merge_repo_candidates(discovered, csv_candidates, existing_repo_map)
        logger.info("fetch_filter.discovery_complete", run_id=run_id, repos=len(repo_candidates), csv_repos=len(csv_candidates))

        for repo_bundle in repo_candidates:
            full_name = repo_bundle["full_name"]
            repo_id = db.upsert_repository(
                {
                    "full_name": full_name,
                    "default_branch": repo_bundle.get("default_branch") or None,
                    "description": None,
                    "language": None,
                    "stars": 0,
                    "forks": 0,
                    "pushed_at": None,
                    "discovery_source": repo_bundle.get("discovery_source"),
                    "readme_uvm_hit": False,
                    "metadata": repo_bundle.get("seed_metadata") or {},
                }
            )

            try:
                metadata = fetcher.fetch_repo_metadata(full_name)
            except Exception as exc:
                logger.warning("fetch_filter.repo_metadata_failed", run_id=run_id, repo=full_name, error=str(exc))
                continue

            readme_text = fetcher.fetch_readme_text(full_name)
            repo_ok, readme_uvm_hit = repo_matches_policy(full_name, metadata, readme_text, config)
            db.upsert_repository(
                {
                    "full_name": full_name,
                    "default_branch": metadata.get("default_branch"),
                    "description": metadata.get("description"),
                    "language": metadata.get("language"),
                    "stars": metadata.get("stargazers_count", 0),
                    "forks": metadata.get("forks_count", 0),
                    "pushed_at": metadata.get("pushed_at"),
                    "discovery_source": repo_bundle.get("discovery_source"),
                    "readme_uvm_hit": readme_uvm_hit,
                    "metadata": {**(repo_bundle.get("seed_metadata") or {}), **metadata},
                }
            )

            default_branch = str(metadata.get("default_branch") or repo_bundle.get("default_branch") or "")
            if default_branch:
                db.record_commit(repo_id=repo_id, commit_sha=default_branch, commit_date=metadata.get("pushed_at"), is_default_head=True)

            if not repo_ok:
                logger.info("fetch_filter.repo_discarded", run_id=run_id, repo=full_name, reason="repo_policy_mismatch")
                continue

            files = discovery.list_repo_files(full_name=full_name, default_branch=default_branch, max_files_per_repo=max_files_per_repo)
            for file_record in files:
                source_url = str(file_record.get("url") or "")
                db.upsert_candidate_file(
                    repo_id,
                    {
                        **file_record,
                        "source_url": source_url,
                        "metadata": {
                            "repo_ok": repo_ok,
                            "blob_sha": file_record.get("blob_sha"),
                            "blob_url": source_url,
                        },
                    },
                )

        for row in db.list_files_for_processing(resume=resume):
            repo = db.get_repository(int(row["repo_id"]))
            if not repo:
                continue
            full_name = str(repo["full_name"])
            file_id = int(row["file_id"])
            ext = str(row["ext"]).lower()
            path = str(row["path"])
            size_bytes = int(row.get("size_bytes") or 0)
            if not extension_allowed(path, config.discovery.extensions):
                db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=None, dedup_pass=None, discard_reason="extension_not_allowed", scores={})
                continue
            if size_bytes and size_bytes > config.filters.max_file_size_kb * 1024:
                db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=None, dedup_pass=None, discard_reason="file_too_large", scores={"size_bytes": size_bytes})
                continue

            try:
                blob_sha = str(row.get("commit_sha") or "")
                blob_url = str(row.get("source_url") or "") or None
                if ext == ".pdf":
                    binary_response = fetcher.fetch_binary_file(full_name=full_name, path=path, blob_sha=blob_sha or None, blob_url=blob_url)
                    if not binary_response.success or binary_response.content is None:
                        db.mark_fetch_attempt(file_id, False, http_status=binary_response.status_code, error_type=binary_response.error_type, error_message=binary_response.error_message)
                        db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=None, dedup_pass=None, discard_reason=binary_response.error_type or "download_failed", scores={"http_status": binary_response.status_code})
                        continue

                    destination = safe_repo_path(config.raw_dir, full_name, path)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(binary_response.content)
                    content_sha = sha256_bytes(binary_response.content)
                    db.mark_fetch_attempt(
                        file_id,
                        True,
                        http_status=binary_response.status_code,
                        etag=binary_response.etag,
                        local_raw_path=str(destination),
                        content_sha256=content_sha,
                    )
                    db.upsert_file_filter(file_id, keyword_pass=True, syntax_pass=True, dedup_pass=None, discard_reason=None, scores={})
                    logger.info("fetch_filter.file_saved", run_id=run_id, repo=full_name, path=path, destination=str(destination))
                    continue

                text_response = fetcher.fetch_text_file(full_name=full_name, path=path, blob_sha=blob_sha or None, blob_url=blob_url)
            except Exception as exc:
                db.mark_fetch_attempt(file_id, False, error_type=type(exc).__name__, error_message=str(exc))
                logger.warning("fetch_filter.file_fetch_failed", run_id=run_id, repo=full_name, path=path, error=str(exc))
                continue

            if not text_response.success or text_response.text is None:
                db.mark_fetch_attempt(file_id, False, http_status=text_response.status_code, error_type=text_response.error_type, error_message=text_response.error_message)
                db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=None, dedup_pass=None, discard_reason=text_response.error_type or "download_failed", scores={"http_status": text_response.status_code})
                continue

            text = text_response.text
            if not has_minimum_text(text, config.filters.min_text_chars):
                db.mark_fetch_attempt(file_id, False, http_status=text_response.status_code, error_type="too_short", error_message="file below min_text_chars")
                db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=None, dedup_pass=None, discard_reason="too_short", scores={"char_count": len(text)})
                continue

            keyword_pass = True
            scores: dict[str, Any] = {}
            if ext in SV_EXTENSIONS:
                keyword_pass, scores = should_keep_sv_file(text, config.filters)
                if not keyword_pass:
                    db.mark_fetch_attempt(file_id, False, http_status=text_response.status_code, error_type="keyword_filter", error_message="sv file missing required keywords")
                    db.upsert_file_filter(file_id, keyword_pass=False, syntax_pass=True, dedup_pass=None, discard_reason="keyword_filter", scores=scores)
                    continue

            dedup_result = compare_against_existing(text, existing_dedup_records, config.dedup)
            db.upsert_file_signature(
                file_id,
                minhash_b64=dedup_result.minhash_b64,
                token_count=dedup_result.token_count,
                shingle_size=config.dedup.shingle_size,
                near_dup_cluster_id=dedup_result.cluster_id,
                jaccard_to_cluster_rep=dedup_result.similarity,
            )
            if config.dedup.enabled and dedup_result.near_duplicate:
                db.mark_fetch_attempt(file_id, False, http_status=text_response.status_code, error_type="near_duplicate", error_message="dedup threshold matched", content_sha256=dedup_result.sha256)
                db.upsert_file_filter(file_id, keyword_pass=keyword_pass, syntax_pass=True, dedup_pass=False, discard_reason="near_duplicate", scores={**scores, "similarity": dedup_result.similarity})
                continue

            destination = safe_repo_path(config.raw_dir, full_name, path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(text, encoding="utf-8")
            db.mark_fetch_attempt(
                file_id,
                True,
                http_status=text_response.status_code,
                etag=text_response.etag,
                local_raw_path=str(destination),
                content_sha256=dedup_result.sha256,
            )
            db.upsert_file_filter(file_id, keyword_pass=keyword_pass, syntax_pass=True, dedup_pass=True, discard_reason=None, scores={**scores, "similarity": dedup_result.similarity})
            existing_dedup_records.append({"text": text, "content_sha256": dedup_result.sha256, "cluster_id": dedup_result.cluster_id})
            logger.info("fetch_filter.file_saved", run_id=run_id, repo=full_name, path=path, destination=str(destination))

        db.finish_pipeline_run(run_id, status="completed")
        return run_id
    except Exception as exc:
        db.finish_pipeline_run(run_id, status="failed", error_summary=str(exc))
        raise
    finally:
        fetcher.close()
