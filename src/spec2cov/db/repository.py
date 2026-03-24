from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

import orjson
from sqlalchemy import Select, and_, delete, desc, insert, select, update
from sqlalchemy.engine import Engine

from spec2cov.db.schema import (
    artifacts,
    candidate_files,
    create_db_engine,
    exports,
    fetch_attempts,
    file_filters,
    file_signatures,
    pipeline_runs,
    repo_commits,
    repo_quality,
    repositories,
    samples,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dumps(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


class Database:
    def __init__(self, db_path: str):
        self.engine: Engine = create_db_engine(db_path)

    @contextmanager
    def begin(self) -> Iterator[Any]:
        with self.engine.begin() as conn:
            yield conn

    def create_pipeline_run(self, stage: str, config_snapshot: dict[str, Any], code_version: str) -> int:
        with self.begin() as conn:
            result = conn.execute(
                insert(pipeline_runs).values(
                    stage=stage,
                    status="running",
                    started_at=_now(),
                    config_snapshot_json=dumps(config_snapshot),
                    code_version=code_version,
                )
            )
            return int(result.inserted_primary_key[0])

    def finish_pipeline_run(self, run_id: int, status: str, error_summary: str | None = None) -> None:
        with self.begin() as conn:
            conn.execute(
                update(pipeline_runs)
                .where(pipeline_runs.c.run_id == run_id)
                .values(status=status, finished_at=_now(), error_summary=error_summary)
            )

    def upsert_repository(self, repo: dict[str, Any]) -> int:
        with self.begin() as conn:
            existing = conn.execute(select(repositories.c.repo_id).where(repositories.c.full_name == repo["full_name"])).scalar_one_or_none()
            payload = {
                "full_name": repo["full_name"],
                "default_branch": repo.get("default_branch"),
                "description": repo.get("description"),
                "language": repo.get("language"),
                "stars": repo.get("stars", 0),
                "forks": repo.get("forks", 0),
                "pushed_at": repo.get("pushed_at"),
                "discovery_source": repo.get("discovery_source"),
                "readme_uvm_hit": bool(repo.get("readme_uvm_hit", False)),
                "metadata_json": dumps(repo.get("metadata", {})),
            }
            if existing is None:
                result = conn.execute(insert(repositories).values(**payload))
                return int(result.inserted_primary_key[0])
            conn.execute(update(repositories).where(repositories.c.repo_id == existing).values(**payload))
            return int(existing)

    def record_commit(self, repo_id: int, commit_sha: str, commit_date: str | None, is_default_head: bool) -> None:
        with self.begin() as conn:
            existing = conn.execute(
                select(repo_commits.c.id).where(and_(repo_commits.c.repo_id == repo_id, repo_commits.c.commit_sha == commit_sha))
            ).scalar_one_or_none()
            payload = {
                "repo_id": repo_id,
                "commit_sha": commit_sha,
                "commit_date": commit_date,
                "is_default_head": is_default_head,
            }
            if existing is None:
                conn.execute(insert(repo_commits).values(**payload))
            else:
                conn.execute(update(repo_commits).where(repo_commits.c.id == existing).values(**payload))

    def upsert_candidate_file(self, repo_id: int, record: dict[str, Any]) -> int:
        with self.begin() as conn:
            stmt: Select[Any] = select(candidate_files.c.file_id).where(
                and_(
                    candidate_files.c.repo_id == repo_id,
                    candidate_files.c.path == record["path"],
                    candidate_files.c.commit_sha == record["commit_sha"],
                )
            )
            existing = conn.execute(stmt).scalar_one_or_none()
            payload = {
                "repo_id": repo_id,
                "path": record["path"],
                "ext": record["ext"],
                "size_bytes": record.get("size_bytes"),
                "source_url": record.get("source_url"),
                "commit_sha": record["commit_sha"],
                "metadata_json": dumps(record.get("metadata", {})),
            }
            if existing is None:
                result = conn.execute(insert(candidate_files).values(**payload))
                return int(result.inserted_primary_key[0])
            conn.execute(update(candidate_files).where(candidate_files.c.file_id == existing).values(**payload))
            return int(existing)

    def list_candidate_files(self, repo_id: int | None = None) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            stmt = select(candidate_files)
            if repo_id is not None:
                stmt = stmt.where(candidate_files.c.repo_id == repo_id)
            rows = conn.execute(stmt).mappings().all()
            return [dict(row) for row in rows]

    def list_files_for_processing(self, repo_id: int | None = None, resume: bool = False) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            last_fetch_success = (
                select(fetch_attempts.c.success)
                .where(fetch_attempts.c.file_id == candidate_files.c.file_id)
                .order_by(desc(fetch_attempts.c.attempt_id))
                .limit(1)
                .scalar_subquery()
            )
            stmt = select(candidate_files, last_fetch_success.label("last_fetch_success"))
            if repo_id is not None:
                stmt = stmt.where(candidate_files.c.repo_id == repo_id)
            if resume:
                stmt = stmt.where((last_fetch_success.is_(None)) | (last_fetch_success.is_(False)))
            rows = conn.execute(stmt).mappings().all()
            return [dict(row) for row in rows]

    def list_repositories(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(repositories)).mappings().all()
            return [dict(row) for row in rows]

    def list_repository_name_map(self) -> dict[str, dict[str, Any]]:
        return {str(row["full_name"]).lower(): row for row in self.list_repositories()}

    def get_repository(self, repo_id: int) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(repositories).where(repositories.c.repo_id == repo_id)).mappings().first()
            return dict(row) if row else None

    def get_repository_by_full_name(self, full_name: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(repositories).where(repositories.c.full_name == full_name)).mappings().first()
            return dict(row) if row else None

    def mark_fetch_attempt(self, file_id: int, success: bool, **kwargs: Any) -> None:
        payload = {
            "file_id": file_id,
            "attempted_at": _now(),
            "success": success,
            "http_status": kwargs.get("http_status"),
            "etag": kwargs.get("etag"),
            "local_raw_path": kwargs.get("local_raw_path"),
            "content_sha256": kwargs.get("content_sha256"),
            "error_type": kwargs.get("error_type"),
            "error_message": kwargs.get("error_message"),
        }
        with self.begin() as conn:
            conn.execute(insert(fetch_attempts).values(**payload))

    def upsert_file_filter(self, file_id: int, **kwargs: Any) -> None:
        with self.begin() as conn:
            existing = conn.execute(select(file_filters.c.file_id).where(file_filters.c.file_id == file_id)).scalar_one_or_none()
            payload = {
                "file_id": file_id,
                "keyword_pass": kwargs.get("keyword_pass"),
                "syntax_pass": kwargs.get("syntax_pass"),
                "dedup_pass": kwargs.get("dedup_pass"),
                "discard_reason": kwargs.get("discard_reason"),
                "scores_json": dumps(kwargs.get("scores", {})),
            }
            if existing is None:
                conn.execute(insert(file_filters).values(**payload))
            else:
                conn.execute(update(file_filters).where(file_filters.c.file_id == file_id).values(**payload))

    def upsert_file_signature(self, file_id: int, **kwargs: Any) -> None:
        with self.begin() as conn:
            existing = conn.execute(select(file_signatures.c.file_id).where(file_signatures.c.file_id == file_id)).scalar_one_or_none()
            payload = {
                "file_id": file_id,
                "minhash_b64": kwargs.get("minhash_b64"),
                "token_count": kwargs.get("token_count"),
                "shingle_size": kwargs.get("shingle_size"),
                "near_dup_cluster_id": kwargs.get("near_dup_cluster_id"),
                "jaccard_to_cluster_rep": kwargs.get("jaccard_to_cluster_rep"),
            }
            if existing is None:
                conn.execute(insert(file_signatures).values(**payload))
            else:
                conn.execute(update(file_signatures).where(file_signatures.c.file_id == file_id).values(**payload))

    def replace_artifacts_for_repo(self, repo_id: int, artifact_rows: list[dict[str, Any]]) -> None:
        with self.begin() as conn:
            conn.execute(delete(artifacts).where(artifacts.c.repo_id == repo_id))
            if artifact_rows:
                conn.execute(insert(artifacts), artifact_rows)

    def list_artifacts(self, repo_id: int | None = None) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            stmt = select(artifacts)
            if repo_id is not None:
                stmt = stmt.where(artifacts.c.repo_id == repo_id)
            rows = conn.execute(stmt).mappings().all()
            return [dict(row) for row in rows]

    def upsert_repo_quality(self, repo_id: int, counts: dict[str, Any], score: float, passed: bool, discard_reason: str | None, thresholds: dict[str, Any]) -> None:
        with self.begin() as conn:
            existing = conn.execute(select(repo_quality.c.repo_id).where(repo_quality.c.repo_id == repo_id)).scalar_one_or_none()
            payload = {
                "repo_id": repo_id,
                "artifact_counts_json": dumps(counts),
                "quality_score": score,
                "pass_quality_gate": passed,
                "discard_reason": discard_reason,
                "thresholds_snapshot_json": dumps(thresholds),
            }
            if existing is None:
                conn.execute(insert(repo_quality).values(**payload))
            else:
                conn.execute(update(repo_quality).where(repo_quality.c.repo_id == repo_id).values(**payload))

    def upsert_sample(self, repo_id: int, sample_key: str, difficulty: str, categories: list[str], input_artifacts: dict[str, Any], output_artifacts: dict[str, Any], build_status: str, discard_reason: str | None = None) -> None:
        with self.begin() as conn:
            existing = conn.execute(select(samples.c.sample_id).where(samples.c.sample_key == sample_key)).scalar_one_or_none()
            payload = {
                "repo_id": repo_id,
                "sample_key": sample_key,
                "difficulty": difficulty,
                "categories_json": dumps(categories),
                "input_artifacts_json": dumps(input_artifacts),
                "output_artifacts_json": dumps(output_artifacts),
                "build_status": build_status,
                "discard_reason": discard_reason,
            }
            if existing is None:
                conn.execute(insert(samples).values(**payload))
            else:
                conn.execute(update(samples).where(samples.c.sample_id == existing).values(**payload))

    def list_samples(self, only_ready: bool = False) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            stmt = select(samples)
            if only_ready:
                stmt = stmt.where(samples.c.build_status == "ready")
            rows = conn.execute(stmt).mappings().all()
            return [dict(row) for row in rows]

    def record_export(self, format_name: str, output_path: str, run_id: int, record_count: int, manifest: dict[str, Any]) -> None:
        with self.begin() as conn:
            conn.execute(
                insert(exports).values(
                    format=format_name,
                    output_path=output_path,
                    created_at=_now(),
                    record_count=record_count,
                    run_id=run_id,
                    manifest_json=dumps(manifest),
                )
            )

    def clear_samples(self) -> None:
        with self.begin() as conn:
            conn.execute(delete(samples))
