from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class RuntimeConfig(BaseModel):
    concurrency: int = 4
    timeout_sec: int = 30
    retry_count: int = 3
    user_agent: str = "spec2cov/0.1"
    github_token_env: str = "GITHUB_TOKEN"


class DiscoveryConfig(BaseModel):
    provider: str = "github"
    max_repos: int = 200
    max_files_per_repo: int = 50
    repo_languages: list[str] = Field(default_factory=lambda: ["Verilog", "SystemVerilog"])
    extensions: list[str] = Field(default_factory=lambda: [".v", ".sv", ".md", ".xml", ".xlsx", ".ralf", ".hvp", ".pdf"])
    uvm_keywords: list[str] = Field(default_factory=lambda: ["UVM", "uvm", "universal verification methodology"])
    excluded_repo_name_keywords: list[str] = Field(default_factory=lambda: ["tool"])
    search_queries: list[str] = Field(default_factory=lambda: [
        "UVM language:Verilog",
        '"SystemVerilog" UVM',
        '"universal verification methodology" verilog',
    ])
    repo_sort: str = "stars"
    repo_order: str = "desc"
    github_api_base: str = "https://api.github.com"
    github_per_page: int = 50
    request_interval_sec: float = 0.8
    low_remaining_threshold: int = 200
    rate_limit_buffer_sec: int = 5
    secondary_limit_wait_sec: int = 60
    secondary_limit_max_retries: int = 3


class FilterConfig(BaseModel):
    sv_keywords: list[str] = Field(default_factory=lambda: ["module", "interface", "covergroup", "bins", "coverpoint", "cover", "cross"])
    md_spec_keywords: list[str] = Field(default_factory=lambda: ["protocol", "behavior", "design", "functional", "feature", "module", "interface"])
    max_file_size_kb: int = 1024
    min_text_chars: int = 40
    project_cluster_file_threshold: int = 10
    project_cluster_path_depth_tolerance: int = 2
    project_cluster_ignore_name_tokens: list[str] = Field(default_factory=lambda: ["tb", "dut", "sim"])


class DedupConfig(BaseModel):
    enabled: bool = True
    shingle_size: int = 5
    minhash_perm: int = 128
    jaccard_threshold: float = 0.85


class QualityGateConfig(BaseModel):
    min_cover_blocks: int = 1
    min_plan_segments: int = 1
    min_spec_chars: int = 120
    min_dut_blocks: int = 1
    min_total_artifacts: int = 3


class ExportConfig(BaseModel):
    dataset_prefix: str = "cvdp"
    default_category: str = "spec-to-coverage"
    default_difficulty: str = "medium"
    prompt_template: str
    system_message: str


class AppConfig(BaseModel):
    project_name: str = "spec2cov"
    code_version: str = "0.1.0"
    data_root: Path = Path("data")
    db_path: Path = Path("data/pipeline.db")
    raw_dir: Path = Path("data/raw")
    preprocess_dir: Path = Path("data/preprocess")
    export_dir: Path = Path("data/exports")
    log_dir: Path = Path("data/logs")
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    filters: FilterConfig = Field(default_factory=FilterConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    quality_gates: QualityGateConfig = Field(default_factory=QualityGateConfig)
    export: ExportConfig

    def resolve_paths(self, root: Path) -> "AppConfig":
        updates = {
            "data_root": _resolve_path(root, self.data_root),
            "db_path": _resolve_path(root, self.db_path),
            "raw_dir": _resolve_path(root, self.raw_dir),
            "preprocess_dir": _resolve_path(root, self.preprocess_dir),
            "export_dir": _resolve_path(root, self.export_dir),
            "log_dir": _resolve_path(root, self.log_dir),
        }
        return self.model_copy(update=updates)

    def snapshot(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _resolve_path(root: Path, value: Path) -> Path:
    return value if value.is_absolute() else (root / value).resolve()


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path).resolve()
    project_root = path.parent.parent if path.parent.name == "config" else path.parent
    load_dotenv(project_root / ".env", override=False)
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    config = AppConfig.model_validate(data)
    return config.resolve_paths(project_root)
