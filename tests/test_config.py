from __future__ import annotations

from pathlib import Path

from spec2cov.config import load_config


def test_load_config_reads_project_root_dotenv(tmp_path: Path):
    project_root = tmp_path / "proj"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    (project_root / ".env").write_text("GITHUB_TOKEN=test-token\n", encoding="utf-8")
    (config_dir / "default.yaml").write_text(
        """
project_name: spec2cov
code_version: 0.1.0
runtime:
  github_token_env: GITHUB_TOKEN
export:
  dataset_prefix: cvdp
  default_category: spec-to-coverage
  default_difficulty: medium
  prompt_template: prompt
  system_message: system
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_dir / "default.yaml")

    assert config.project_name == "spec2cov"
