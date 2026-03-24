from __future__ import annotations

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, MetaData, String, Table, Text, UniqueConstraint, create_engine
from sqlalchemy.engine import Engine

metadata = MetaData()

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("stage", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    Column("started_at", String(64), nullable=False),
    Column("finished_at", String(64)),
    Column("config_snapshot_json", Text, nullable=False),
    Column("error_summary", Text),
    Column("code_version", String(32), nullable=False),
)

repositories = Table(
    "repositories",
    metadata,
    Column("repo_id", Integer, primary_key=True, autoincrement=True),
    Column("full_name", String(255), nullable=False, unique=True),
    Column("default_branch", String(255)),
    Column("description", Text),
    Column("language", String(64)),
    Column("stars", Integer, default=0),
    Column("forks", Integer, default=0),
    Column("pushed_at", String(64)),
    Column("discovery_source", String(255)),
    Column("readme_uvm_hit", Boolean, default=False),
    Column("metadata_json", Text, nullable=False, default="{}"),
)

repo_commits = Table(
    "repo_commits",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("repo_id", Integer, ForeignKey("repositories.repo_id"), nullable=False),
    Column("commit_sha", String(64), nullable=False),
    Column("commit_date", String(64)),
    Column("is_default_head", Boolean, default=False),
    UniqueConstraint("repo_id", "commit_sha", name="uq_repo_commit"),
)

candidate_files = Table(
    "candidate_files",
    metadata,
    Column("file_id", Integer, primary_key=True, autoincrement=True),
    Column("repo_id", Integer, ForeignKey("repositories.repo_id"), nullable=False),
    Column("path", Text, nullable=False),
    Column("ext", String(16), nullable=False),
    Column("size_bytes", Integer),
    Column("source_url", Text),
    Column("commit_sha", String(64), nullable=False),
    Column("metadata_json", Text, nullable=False, default="{}"),
    UniqueConstraint("repo_id", "path", "commit_sha", name="uq_candidate_file"),
)

fetch_attempts = Table(
    "fetch_attempts",
    metadata,
    Column("attempt_id", Integer, primary_key=True, autoincrement=True),
    Column("file_id", Integer, ForeignKey("candidate_files.file_id"), nullable=False),
    Column("attempted_at", String(64), nullable=False),
    Column("success", Boolean, nullable=False),
    Column("http_status", Integer),
    Column("etag", String(255)),
    Column("local_raw_path", Text),
    Column("content_sha256", String(64)),
    Column("error_type", String(64)),
    Column("error_message", Text),
)

file_filters = Table(
    "file_filters",
    metadata,
    Column("file_id", Integer, ForeignKey("candidate_files.file_id"), primary_key=True),
    Column("keyword_pass", Boolean),
    Column("syntax_pass", Boolean),
    Column("dedup_pass", Boolean),
    Column("discard_reason", Text),
    Column("scores_json", Text, nullable=False, default="{}"),
)

file_signatures = Table(
    "file_signatures",
    metadata,
    Column("file_id", Integer, ForeignKey("candidate_files.file_id"), primary_key=True),
    Column("minhash_b64", Text),
    Column("token_count", Integer),
    Column("shingle_size", Integer),
    Column("near_dup_cluster_id", String(128)),
    Column("jaccard_to_cluster_rep", Float),
)

artifacts = Table(
    "artifacts",
    metadata,
    Column("artifact_id", Integer, primary_key=True, autoincrement=True),
    Column("repo_id", Integer, ForeignKey("repositories.repo_id"), nullable=False),
    Column("file_id", Integer, ForeignKey("candidate_files.file_id")),
    Column("artifact_type", String(16), nullable=False),
    Column("artifact_name", String(255), nullable=False),
    Column("content_path", Text, nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("char_count", Integer, nullable=False),
    Column("span_json", Text, nullable=False, default="{}"),
    Column("parser_json", Text, nullable=False, default="{}"),
    Column("metadata_json", Text, nullable=False, default="{}"),
    UniqueConstraint("repo_id", "artifact_name", "artifact_type", name="uq_artifact"),
)

repo_quality = Table(
    "repo_quality",
    metadata,
    Column("repo_id", Integer, ForeignKey("repositories.repo_id"), primary_key=True),
    Column("artifact_counts_json", Text, nullable=False),
    Column("quality_score", Float, nullable=False),
    Column("pass_quality_gate", Boolean, nullable=False),
    Column("discard_reason", Text),
    Column("thresholds_snapshot_json", Text, nullable=False),
)

samples = Table(
    "samples",
    metadata,
    Column("sample_id", Integer, primary_key=True, autoincrement=True),
    Column("repo_id", Integer, ForeignKey("repositories.repo_id"), nullable=False),
    Column("sample_key", String(255), nullable=False, unique=True),
    Column("difficulty", String(64), nullable=False),
    Column("categories_json", Text, nullable=False),
    Column("input_artifacts_json", Text, nullable=False),
    Column("output_artifacts_json", Text, nullable=False),
    Column("build_status", String(32), nullable=False),
    Column("discard_reason", Text),
)

exports = Table(
    "exports",
    metadata,
    Column("export_id", Integer, primary_key=True, autoincrement=True),
    Column("format", String(32), nullable=False),
    Column("output_path", Text, nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("record_count", Integer, nullable=False),
    Column("run_id", Integer, ForeignKey("pipeline_runs.run_id"), nullable=False),
    Column("manifest_json", Text, nullable=False),
)


def create_db_engine(db_path: str) -> Engine:
    return create_engine(f"sqlite:///{db_path}", future=True)


def create_all(engine: Engine) -> None:
    metadata.create_all(engine)
