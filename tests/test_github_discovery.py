from __future__ import annotations

from pathlib import Path

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.db.repository import Database
from spec2cov.db.schema import create_all
from spec2cov.sources.github_discovery import GitHubDiscovery, normalize_repo_ref
from spec2cov.stages.fetch_filter import repo_matches_policy


class DummyFetcher:
    def search_repositories(self, query: str, limit: int):
        return []

    def list_matching_files(self, full_name: str, default_branch: str, limit: int, extensions: list[str]):
        return []


def make_config() -> AppConfig:
    return AppConfig(
        export=ExportConfig(
            dataset_prefix="cvdp",
            default_category="spec-to-coverage",
            default_difficulty="medium",
            prompt_template="prompt",
            system_message="system",
        )
    )


def test_normalize_repo_ref_accepts_github_urls_and_owner_repo():
    assert normalize_repo_ref("owner/repo") == "owner/repo"
    assert normalize_repo_ref("https://github.com/owner/repo") == "owner/repo"
    assert normalize_repo_ref("github.com/owner/repo.git") == "owner/repo"
    assert normalize_repo_ref("https://github.com/owner/repo?tab=readme") == "owner/repo"


def test_normalize_repo_ref_rejects_invalid_values():
    assert normalize_repo_ref("") is None
    assert normalize_repo_ref("https://example.com/owner/repo") is None
    assert normalize_repo_ref("owner") is None


def test_load_repo_candidates_from_csv_deduplicates_rows(tmp_path: Path):
    csv_path = tmp_path / "repos.csv"
    csv_path.write_text(
        "repo_url\nhttps://github.com/foo/bar\nfoo/bar\nhttps://github.com/baz/qux.git\n",
        encoding="utf-8",
    )
    discovery = GitHubDiscovery(make_config(), DummyFetcher())

    rows = discovery.load_repo_candidates_from_csv(csv_path)

    assert [row["full_name"] for row in rows] == ["foo/bar", "baz/qux"]


def test_list_repository_name_map_is_case_insensitive(tmp_path: Path):
    db_path = tmp_path / "pipeline.db"
    db = Database(str(db_path))
    create_all(db.engine)
    db.upsert_repository(
        {
            "full_name": "Owner/Repo",
            "default_branch": "main",
            "description": None,
            "language": None,
            "stars": 0,
            "forks": 0,
            "pushed_at": None,
            "discovery_source": "test",
            "readme_uvm_hit": False,
            "metadata": {},
        }
    )

    name_map = db.list_repository_name_map()

    assert "owner/repo" in name_map
    assert name_map["owner/repo"]["full_name"] == "Owner/Repo"


def test_repo_matches_policy_rejects_repo_names_containing_tool():
    config = make_config()

    keep, uvm_hit = repo_matches_policy(
        "foo/uvm-toolkit",
        {"language": "Verilog", "description": "UVM verification environment"},
        "UVM README",
        config,
    )

    assert keep is False
    assert uvm_hit is True
