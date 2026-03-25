from __future__ import annotations

from pathlib import Path

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.db.repository import Database
from spec2cov.db.schema import create_all
from spec2cov.stages import gen_retrieve


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


def test_gen_retrieve_collapses_more_than_two_blank_lines_in_preprocess_files(tmp_path: Path):
    config = make_config(tmp_path)
    for path in [config.data_root, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)

    repo_dir = config.preprocess_dir / "owner__repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = repo_dir / "proj0001-demo-spec.txt"
    artifact_path.write_text("alpha\n\n\n\nbeta\n\n\ngamma\n", encoding="utf-8")

    gen_retrieve.run(config)

    assert artifact_path.read_text(encoding="utf-8") == "alpha\n\nbeta\n\ngamma\n"
