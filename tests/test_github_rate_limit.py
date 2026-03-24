from __future__ import annotations

from unittest.mock import Mock, patch

import httpx

import base64

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.sources.github_fetcher import GitHubFetcher


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


def test_request_sleeps_until_reset_when_primary_rate_limit_exceeded():
    config = make_config()
    fetcher = GitHubFetcher(config)

    first = Mock(spec=httpx.Response)
    first.status_code = 403
    first.headers = {
        "x-ratelimit-remaining": "0",
        "x-ratelimit-reset": "110",
        "x-ratelimit-resource": "core",
        "x-ratelimit-used": "5000",
    }

    second = Mock(spec=httpx.Response)
    second.status_code = 200
    second.headers = {
        "x-ratelimit-remaining": "4999",
        "x-ratelimit-reset": "3600",
        "x-ratelimit-resource": "core",
        "x-ratelimit-used": "1",
    }

    with patch("spec2cov.sources.github_fetcher.time.time", return_value=100), patch(
        "spec2cov.sources.github_fetcher.time.sleep"
    ) as sleep_mock:
        fetcher.client.request = Mock(side_effect=[first, second])
        response = fetcher._request("GET", "/search/repositories")

    assert response is second
    sleep_mock.assert_any_call(config.discovery.rate_limit_buffer_sec + 10)
    assert fetcher.client.request.call_count == 2


def test_request_uses_retry_after_when_present():
    config = make_config()
    fetcher = GitHubFetcher(config)

    first = Mock(spec=httpx.Response)
    first.status_code = 429
    first.headers = {
        "retry-after": "7",
        "x-ratelimit-resource": "search",
    }

    second = Mock(spec=httpx.Response)
    second.status_code = 200
    second.headers = {
        "x-ratelimit-remaining": "4990",
        "x-ratelimit-reset": "3600",
        "x-ratelimit-resource": "search",
    }

    with patch("spec2cov.sources.github_fetcher.time.sleep") as sleep_mock:
        fetcher.client.request = Mock(side_effect=[first, second])
        response = fetcher._request("GET", "/search/repositories")

    assert response is second
    sleep_mock.assert_any_call(7)


def test_parse_next_link_returns_next_url():
    response = Mock(spec=httpx.Response)
    response.headers = {
        "link": '<https://api.github.com/search/repositories?q=uvm&page=2>; rel="next", <https://api.github.com/search/repositories?q=uvm&page=34>; rel="last"'
    }

    assert GitHubFetcher._parse_next_link(response) == "https://api.github.com/search/repositories?q=uvm&page=2"


def test_request_logs_low_remaining_without_sleeping():
    config = make_config()
    fetcher = GitHubFetcher(config)

    response_mock = Mock(spec=httpx.Response)
    response_mock.status_code = 200
    response_mock.headers = {
        "x-ratelimit-remaining": str(config.discovery.low_remaining_threshold),
        "x-ratelimit-reset": "3600",
        "x-ratelimit-resource": "core",
        "x-ratelimit-used": "4800",
    }

    with patch("spec2cov.sources.github_fetcher.time.sleep") as sleep_mock:
        fetcher._log_rate_limit_status(response_mock)

    sleep_mock.assert_not_called()


def test_fetch_binary_file_decodes_base64_bytes():
    config = make_config()
    fetcher = GitHubFetcher(config)
    pdf_bytes = b"%PDF-1.4\nmock-pdf"

    response_mock = Mock(spec=httpx.Response)
    response_mock.status_code = 200
    response_mock.headers = {"etag": "abc"}
    response_mock.json.return_value = {
        "encoding": "base64",
        "content": base64.b64encode(pdf_bytes).decode("ascii"),
    }

    with patch.object(fetcher, "_request", return_value=response_mock):
        response = fetcher.fetch_binary_file("owner/repo", "spec.pdf", blob_sha="sha")

    assert response.success is True
    assert response.content == pdf_bytes
    assert response.etag == "abc"
