from __future__ import annotations

import base64
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from spec2cov.config import AppConfig


@dataclass(slots=True)
class FetchResponse:
    success: bool
    status_code: int
    text: str | None = None
    etag: str | None = None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(slots=True)
class BinaryFetchResponse:
    success: bool
    status_code: int
    content: bytes | None = None
    etag: str | None = None
    error_type: str | None = None
    error_message: str | None = None


class GitHubRateLimitError(RuntimeError):
    pass


class GitHubFetcher:
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._last_request_monotonic = 0.0
        token = os.getenv(config.runtime.github_token_env)
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": config.runtime.user_agent,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self.client = httpx.Client(
            headers=headers,
            timeout=config.runtime.timeout_sec,
            follow_redirects=True,
            base_url=config.discovery.github_api_base,
        )

    def close(self) -> None:
        self.client.close()

    def _throttle_requests(self) -> None:
        interval = self.config.discovery.request_interval_sec
        if interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        if self._last_request_monotonic and elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_request_monotonic = time.monotonic()

    def _compute_wait_seconds(self, response: httpx.Response) -> tuple[int | None, str]:
        retry_after_raw = response.headers.get("retry-after")
        if retry_after_raw and retry_after_raw.isdigit():
            return int(retry_after_raw), "retry-after"

        remaining_raw = response.headers.get("x-ratelimit-remaining")
        reset_raw = response.headers.get("x-ratelimit-reset")
        if remaining_raw == "0" and reset_raw and reset_raw.isdigit():
            return max(int(reset_raw) - int(time.time()) + self.config.discovery.rate_limit_buffer_sec, 1), "x-ratelimit-reset"

        return self.config.discovery.secondary_limit_wait_sec, "secondary-backoff"

    def _log_rate_limit_status(self, response: httpx.Response) -> None:
        remaining_raw = response.headers.get("x-ratelimit-remaining")
        reset_raw = response.headers.get("x-ratelimit-reset")
        resource = response.headers.get("x-ratelimit-resource", "unknown")
        used_raw = response.headers.get("x-ratelimit-used")

        remaining = int(remaining_raw) if remaining_raw and remaining_raw.isdigit() else None
        reset_at = int(reset_raw) if reset_raw and reset_raw.isdigit() else None
        used = int(used_raw) if used_raw and used_raw.isdigit() else None

        if remaining is not None and remaining <= self.config.discovery.low_remaining_threshold:
            self.logger.warning(
                "github rate limit getting low: resource=%s remaining=%s used=%s reset_at=%s",
                resource,
                remaining,
                used,
                reset_at,
            )

    def _handle_rate_limit(self, response: httpx.Response) -> bool:
        if response.status_code not in (403, 429):
            return False

        wait_seconds, wait_reason = self._compute_wait_seconds(response)
        resource = response.headers.get("x-ratelimit-resource", "unknown")
        remaining = response.headers.get("x-ratelimit-remaining")
        used = response.headers.get("x-ratelimit-used")
        reset_at = response.headers.get("x-ratelimit-reset")

        if wait_seconds is None:
            raise GitHubRateLimitError("GitHub rate limit exceeded and no retry information was provided")

        self.logger.warning(
            "github rate limit hit: resource=%s remaining=%s used=%s status=%s wait_reason=%s sleeping=%ss reset_at=%s",
            resource,
            remaining,
            used,
            response.status_code,
            wait_reason,
            wait_seconds,
            reset_at,
        )
        time.sleep(wait_seconds)
        return True

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        attempts = 0
        max_attempts = self.config.discovery.secondary_limit_max_retries + 1

        while attempts < max_attempts:
            self._throttle_requests()
            response = self.client.request(method, url, **kwargs)
            self._log_rate_limit_status(response)

            if not self._handle_rate_limit(response):
                return response

            attempts += 1

        raise GitHubRateLimitError(f"GitHub API request kept hitting rate limits after {self.config.discovery.secondary_limit_max_retries} retries")

    @staticmethod
    def _parse_next_link(response: httpx.Response) -> str | None:
        link_header = response.headers.get("link", "")
        for part in link_header.split(","):
            section = part.strip()
            if 'rel="next"' not in section:
                continue
            if section.startswith("<") and ">" in section:
                return section[1 : section.index(">")]
        return None

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), retry=retry_if_exception_type(httpx.HTTPError))
    def fetch_repo_metadata(self, full_name: str) -> dict[str, Any]:
        response = self._request("GET", f"/repos/{full_name}")
        response.raise_for_status()
        return response.json()

    def fetch_readme_text(self, full_name: str) -> str:
        response = self._request("GET", f"/repos/{full_name}/readme")
        if response.status_code >= 400:
            return ""
        payload = response.json()
        content = payload.get("content")
        if not content:
            return ""
        try:
            return base64.b64decode(content).decode("utf-8", errors="ignore")
        except Exception:
            return ""

    def search_repositories(self, query: str, limit: int) -> list[dict[str, Any]]:
        per_page = min(self.config.discovery.github_per_page, max(limit, 1), 100)
        results: list[dict[str, Any]] = []
        next_url: str | None = "/search/repositories"
        params: dict[str, Any] | None = {
            "q": query,
            "sort": self.config.discovery.repo_sort,
            "order": self.config.discovery.repo_order,
            "per_page": per_page,
            "page": 1,
        }

        while next_url and len(results) < limit:
            response = self._request("GET", next_url, params=params)
            response.raise_for_status()
            items = response.json().get("items", [])
            if not items:
                break
            results.extend(items)
            next_url = self._parse_next_link(response)
            params = None
        return results[:limit]

    def list_matching_files(self, full_name: str, default_branch: str, limit: int, extensions: list[str]) -> list[dict[str, Any]]:
        response = self._request("GET", f"/repos/{full_name}/git/trees/{default_branch}", params={"recursive": "1"})
        if response.status_code >= 400:
            return []
        tree = response.json().get("tree", [])
        allowed = {ext.lower() for ext in extensions}
        matches: list[dict[str, Any]] = []
        for entry in tree:
            if entry.get("type") != "blob":
                continue
            path = str(entry.get("path") or "")
            ext = os.path.splitext(path)[1].lower()
            if ext not in allowed:
                continue
            matches.append(
                {
                    "path": path,
                    "ext": ext,
                    "size_bytes": int(entry.get("size") or 0),
                    "commit_sha": str(entry.get("sha") or default_branch),
                    "blob_sha": str(entry.get("sha") or ""),
                    "url": entry.get("url"),
                }
            )
            if len(matches) >= limit:
                break
        return matches

    def _resolve_blob_url(self, full_name: str, blob_sha: str | None = None, blob_url: str | None = None) -> str | None:
        url = blob_url or (f"/repos/{full_name}/git/blobs/{blob_sha}" if blob_sha else None)
        return url if url else None

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), retry=retry_if_exception_type(httpx.HTTPError))
    def fetch_text_file(self, full_name: str, path: str, blob_sha: str | None = None, blob_url: str | None = None) -> FetchResponse:
        url = self._resolve_blob_url(full_name, blob_sha=blob_sha, blob_url=blob_url)
        if not url:
            return FetchResponse(success=False, status_code=0, error_type="missing_blob_reference", error_message=f"no blob reference for {path}")

        response = self._request("GET", url)
        if response.status_code >= 400:
            return FetchResponse(success=False, status_code=response.status_code, error_type="http_error", error_message=response.text[:500])

        payload = response.json()
        content = payload.get("content")
        encoding = payload.get("encoding")
        if encoding != "base64" or not content:
            return FetchResponse(success=False, status_code=response.status_code, error_type="unsupported_encoding", error_message=f"encoding={encoding}")

        try:
            text = base64.b64decode(content).decode("utf-8", errors="ignore")
        except Exception as exc:
            return FetchResponse(success=False, status_code=response.status_code, error_type="decode_error", error_message=str(exc))

        return FetchResponse(success=True, status_code=response.status_code, text=text, etag=response.headers.get("etag"))

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), retry=retry_if_exception_type(httpx.HTTPError))
    def fetch_binary_file(self, full_name: str, path: str, blob_sha: str | None = None, blob_url: str | None = None) -> BinaryFetchResponse:
        url = self._resolve_blob_url(full_name, blob_sha=blob_sha, blob_url=blob_url)
        if not url:
            return BinaryFetchResponse(success=False, status_code=0, error_type="missing_blob_reference", error_message=f"no blob reference for {path}")

        response = self._request("GET", url)
        if response.status_code >= 400:
            return BinaryFetchResponse(success=False, status_code=response.status_code, error_type="http_error", error_message=response.text[:500])

        payload = response.json()
        content = payload.get("content")
        encoding = payload.get("encoding")
        if encoding != "base64" or not content:
            return BinaryFetchResponse(success=False, status_code=response.status_code, error_type="unsupported_encoding", error_message=f"encoding={encoding}")

        try:
            content_bytes = base64.b64decode(content)
        except Exception as exc:
            return BinaryFetchResponse(success=False, status_code=response.status_code, error_type="decode_error", error_message=str(exc))

        return BinaryFetchResponse(success=True, status_code=response.status_code, content=content_bytes, etag=response.headers.get("etag"))
