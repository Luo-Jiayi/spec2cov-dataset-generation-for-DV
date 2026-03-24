from __future__ import annotations

from collections import defaultdict
from typing import Any

from spec2cov.config import AppConfig


class BigQueryDiscovery:
    def __init__(self, config: AppConfig):
        self.config = config

    def discover(self, max_repos: int | None = None, max_files_per_repo: int | None = None) -> list[dict[str, Any]]:
        if not self.config.bigquery.enabled:
            return []

        from google.cloud import bigquery

        client = bigquery.Client(project=self.config.bigquery.project_id)
        repo_limit = max_repos or self.config.bigquery.max_repos
        file_limit = max_files_per_repo or self.config.bigquery.max_files_per_repo
        query = self._build_query(repo_limit=repo_limit, file_limit=file_limit)
        job = client.query(query)
        rows = list(job.result(timeout=self.config.bigquery.timeout_sec))

        grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"files": []})
        for row in rows:
            full_name = str(row["full_name"])
            entry = grouped[full_name]
            entry["full_name"] = full_name
            entry["discovery_source"] = f"bigquery:{job.job_id}"
            entry["files"].append(
                {
                    "path": str(row["path"]),
                    "ext": str(row["ext"]),
                    "size_bytes": int(row["size_bytes"] or 0),
                    "commit_sha": str(row["commit_sha"]),
                }
            )

        return list(grouped.values())

    def _build_query(self, repo_limit: int, file_limit: int) -> str:
        extensions = "|".join(ext.lstrip(".").lower() for ext in self.config.bigquery.extensions)
        candidate_rows = max(repo_limit * file_limit * 4, 1000)
        dataset = self.config.bigquery.dataset
        return f"""
WITH ranked_files AS (
  SELECT
    repo_name AS full_name,
    path,
    REGEXP_EXTRACT(LOWER(path), r'(\\.[a-z0-9]+)$') AS ext,
    size AS size_bytes,
    id AS commit_sha,
    ROW_NUMBER() OVER (PARTITION BY repo_name ORDER BY size DESC, path ASC) AS file_rank
  FROM `{dataset}.files`
  WHERE REGEXP_CONTAINS(LOWER(path), r'\\.({extensions})$')
)
SELECT full_name, path, ext, size_bytes, commit_sha
FROM ranked_files
WHERE file_rank <= {file_limit}
ORDER BY full_name, file_rank
LIMIT {candidate_rows}
"""
