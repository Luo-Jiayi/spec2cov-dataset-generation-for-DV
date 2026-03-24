from __future__ import annotations

import csv
import io
from collections import OrderedDict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from spec2cov.config import AppConfig
from spec2cov.sources.github_fetcher import GitHubFetcher


def normalize_repo_ref(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None

    if "github.com" in candidate and "://" not in candidate:
        candidate = f"https://{candidate}"

    if "://" in candidate:
        parsed = urlparse(candidate)
        if parsed.netloc.lower() not in {"github.com", "www.github.com"}:
            return None
        parts = [part for part in parsed.path.split("/") if part]
    else:
        parts = [part for part in candidate.split("/") if part]

    if len(parts) < 2:
        return None

    owner, repo = parts[0].strip(), parts[1].strip()
    if not owner or not repo or owner in {".", ".."} or repo in {".", ".."}:
        return None
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not repo:
        return None
    return f"{owner}/{repo}"


class GitHubDiscovery:
    def __init__(self, config: AppConfig, fetcher: GitHubFetcher):
        self.config = config
        self.fetcher = fetcher

    def discover(self, max_repos: int | None = None) -> list[dict[str, Any]]:
        repo_limit = max_repos or self.config.discovery.max_repos
        discovered: OrderedDict[str, dict[str, Any]] = OrderedDict()

        for query in self.config.discovery.search_queries:
            repositories = self.fetcher.search_repositories(query=query, limit=repo_limit)
            for repo in repositories:
                full_name = normalize_repo_ref(str(repo.get("full_name") or ""))
                if not full_name:
                    continue
                key = full_name.lower()
                if key in discovered:
                    continue
                discovered[key] = {
                    "full_name": full_name,
                    "default_branch": str(repo.get("default_branch") or ""),
                    "discovery_source": f"github-search:{query}",
                    "seed_metadata": {
                        "search_query": query,
                        "html_url": repo.get("html_url"),
                        "api_url": repo.get("url"),
                        "description": repo.get("description"),
                        "language": repo.get("language"),
                        "stars": repo.get("stargazers_count", 0),
                        "forks": repo.get("forks_count", 0),
                    },
                }
                if len(discovered) >= repo_limit:
                    return list(discovered.values())
        return list(discovered.values())

    def load_repo_candidates_from_csv(self, csv_path: Path) -> list[dict[str, Any]]:
        text = csv_path.read_text(encoding="utf-8")
        if not text.strip():
            return []

        discovered: OrderedDict[str, dict[str, Any]] = OrderedDict()
        if self._has_header(text):
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                raw_value = self._pick_repo_field(row)
                if not raw_value:
                    continue
                self._add_csv_candidate(discovered, raw_value, csv_path.name, row)
        else:
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if not row:
                    continue
                self._add_csv_candidate(discovered, row[0], csv_path.name, {"value": row[0]})
        return list(discovered.values())

    def list_repo_files(self, full_name: str, default_branch: str, max_files_per_repo: int | None = None) -> list[dict[str, Any]]:
        file_limit = max_files_per_repo or self.config.discovery.max_files_per_repo
        return self.fetcher.list_matching_files(
            full_name=full_name,
            default_branch=default_branch or "HEAD",
            limit=file_limit,
            extensions=self.config.discovery.extensions,
        )

    @staticmethod
    def _has_header(text: str) -> bool:
        try:
            return csv.Sniffer().has_header(text[:2048])
        except csv.Error:
            return False

    @staticmethod
    def _pick_repo_field(row: dict[str, Any]) -> str | None:
        preferred = ["repo_url", "repo", "url", "full_name"]
        for key in preferred:
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value
        for value in row.values():
            if isinstance(value, str) and value.strip():
                return value
        return None

    @staticmethod
    def _add_csv_candidate(discovered: OrderedDict[str, dict[str, Any]], raw_value: str, file_name: str, row: dict[str, Any]) -> None:
        full_name = normalize_repo_ref(raw_value)
        if not full_name:
            return
        key = full_name.lower()
        if key in discovered:
            return
        discovered[key] = {
            "full_name": full_name,
            "default_branch": "",
            "discovery_source": f"github-csv:{file_name}",
            "seed_metadata": {"csv_row": row},
        }
