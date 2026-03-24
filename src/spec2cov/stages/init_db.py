from __future__ import annotations

from pathlib import Path

from spec2cov.config import AppConfig
from spec2cov.db.schema import create_all
from spec2cov.db.repository import Database
from spec2cov.logging_utils import get_logger


def ensure_directories(config: AppConfig) -> None:
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        Path(path).mkdir(parents=True, exist_ok=True)


def run(config: AppConfig) -> int:
    logger = get_logger(__name__)
    ensure_directories(config)
    db = Database(str(config.db_path))
    create_all(db.engine)
    run_id = db.create_pipeline_run(stage="init-db", config_snapshot=config.snapshot(), code_version=config.code_version)
    db.finish_pipeline_run(run_id, status="completed")
    logger.info("init_db.completed", run_id=run_id, db_path=str(config.db_path))
    return run_id
