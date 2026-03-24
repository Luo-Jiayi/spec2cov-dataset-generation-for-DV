from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.db.repository import Database
from spec2cov.db.schema import create_all, fetch_attempts, file_filters, file_signatures
from spec2cov.sources.github_fetcher import BinaryFetchResponse
from spec2cov.stages import fetch_filter


class DummyFetcher:
    def __init__(self, _config: AppConfig):
        pass

    def close(self) -> None:
        return None

    def fetch_repo_metadata(self, full_name: str):
        return {
            "default_branch": "main",
            "description": "uvm repo",
            "language": "Verilog",
            "stargazers_count": 1,
            "forks_count": 0,
            "pushed_at": None,
        }

    def fetch_readme_text(self, full_name: str) -> str:
        return "UVM README"

    def fetch_binary_file(self, full_name: str, path: str, blob_sha: str | None = None, blob_url: str | None = None):
        return BinaryFetchResponse(success=True, status_code=200, content=b"%PDF-1.4\nmock-pdf", etag="etag")

    def fetch_text_file(self, full_name: str, path: str, blob_sha: str | None = None, blob_url: str | None = None):
        raise AssertionError("text fetch should not be used for pdf")


class DummyDiscovery:
    def __init__(self, _config: AppConfig, _fetcher: DummyFetcher):
        pass

    def discover(self, max_repos: int | None = None):
        return [{"full_name": "owner/uvm_repo", "default_branch": "main", "discovery_source": "test", "seed_metadata": {}}]

    def load_repo_candidates_from_csv(self, csv_path: Path):
        return []

    def list_repo_files(self, full_name: str, default_branch: str, max_files_per_repo: int | None = None):
        return [{"path": "docs/spec.pdf", "ext": ".pdf", "size_bytes": 32, "commit_sha": "blobsha", "blob_sha": "blobsha", "url": "/repos/owner/repo/git/blobs/blobsha"}]


def make_config(tmp_path: Path) -> AppConfig:
    data_root = tmp_path / "data"
    return AppConfig(
        data_root=data_root,
        db_path=data_root / "pipeline.db",
        raw_dir=data_root / "raw",
        preprocess_dir=data_root / "preprocess",
        export_dir=data_root / "exports",
        log_dir=data_root / "logs",
        export=ExportConfig(
            dataset_prefix="cvdp",
            default_category="spec-to-coverage",
            default_difficulty="medium",
            prompt_template="prompt",
            system_message="system",
        ),
    )


def test_fetch_filter_saves_pdf_as_bytes(monkeypatch, tmp_path: Path):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)

    monkeypatch.setattr(fetch_filter, "GitHubFetcher", DummyFetcher)
    monkeypatch.setattr(fetch_filter, "GitHubDiscovery", DummyDiscovery)

    run_id = fetch_filter.run(config)

    assert run_id > 0
    pdf_path = config.raw_dir / "owner__uvm_repo" / "docs" / "spec.pdf"
    assert pdf_path.exists()
    assert pdf_path.read_bytes().startswith(b"%PDF")

    files = db.list_candidate_files()
    assert files and files[0]["ext"] == ".pdf"
    file_id = int(files[0]["file_id"])

    with db.engine.connect() as conn:
        attempt = conn.execute(select(fetch_attempts).where(fetch_attempts.c.file_id == file_id)).mappings().first()
        file_filter = conn.execute(select(file_filters).where(file_filters.c.file_id == file_id)).mappings().first()
        signature = conn.execute(select(file_signatures).where(file_signatures.c.file_id == file_id)).mappings().first()

    assert attempt is not None
    assert attempt["success"] is True
    assert attempt["content_sha256"]
    assert file_filter is not None
    assert file_filter["dedup_pass"] is None
    assert signature is None
