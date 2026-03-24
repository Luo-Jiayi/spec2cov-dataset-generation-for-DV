from __future__ import annotations

from pathlib import Path

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.db.repository import Database
from spec2cov.db.schema import create_all
from spec2cov.stages import preprocess


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


def seed_repo(db: Database, full_name: str) -> int:
    return db.upsert_repository(
        {
            "full_name": full_name,
            "default_branch": "main",
            "description": "repo",
            "language": "Verilog",
            "stars": 0,
            "forks": 0,
            "pushed_at": None,
            "discovery_source": "test",
            "readme_uvm_hit": True,
            "metadata": {},
        }
    )


def populate_multi_project_repo(config: AppConfig, db: Database, repo_id: int) -> None:
    repo_dir = config.raw_dir / "owner__multi"
    (repo_dir / "projA").mkdir(parents=True, exist_ok=True)
    (repo_dir / "projB").mkdir(parents=True, exist_ok=True)

    (repo_dir / "projA" / "fifo_plan.xml").write_text("<root><item><name>buffer_cover</name><value>head_ptr_f</value></item></root>", encoding="utf-8")
    (repo_dir / "projA" / "tb_fifo.sv").write_text("covergroup buffer_cover; head: coverpoint head_ptr_f; endgroup\nmodule fifo_dut(input logic head_ptr_f); endmodule", encoding="utf-8")
    (repo_dir / "projA" / "fifo_spec.md").write_text("The head_ptr_f pointer advances in FIFO buffer_cover logic.", encoding="utf-8")

    (repo_dir / "projB" / "packet_plan.xml").write_text("<root><item><name>packet_cover</name><value>tail_ptr_f</value></item></root>", encoding="utf-8")
    (repo_dir / "projB" / "tb_packet.sv").write_text("covergroup packet_cover; tail: coverpoint tail_ptr_f; endgroup\nmodule packet_dut(input logic tail_ptr_f); endmodule", encoding="utf-8")
    (repo_dir / "projB" / "packet_spec.md").write_text("The tail_ptr_f path is described for packet_cover behavior.", encoding="utf-8")

    for rel_path, ext, sha in [
        ("projA/fifo_plan.xml", ".xml", "1"),
        ("projA/tb_fifo.sv", ".sv", "2"),
        ("projA/fifo_spec.md", ".md", "3"),
        ("projB/packet_plan.xml", ".xml", "4"),
        ("projB/tb_packet.sv", ".sv", "5"),
        ("projB/packet_spec.md", ".md", "6"),
    ]:
        db.upsert_candidate_file(repo_id, {"path": rel_path, "ext": ext, "size_bytes": 10, "source_url": "", "commit_sha": sha, "metadata": {}})


def test_preprocess_uses_single_project_when_file_count_is_below_threshold(tmp_path: Path):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/multi")
    populate_multi_project_repo(config, db, repo_id)

    preprocess.run(config)

    preprocess_repo_dir = config.preprocess_dir / "owner__multi"
    names = sorted(path.name for path in preprocess_repo_dir.glob("*.txt"))
    assert names
    assert all(name.startswith("proj0001-") for name in names)


def test_preprocess_uses_multiple_project_indices_when_threshold_is_lowered(tmp_path: Path):
    config = make_config(tmp_path)
    config.filters.project_cluster_file_threshold = 5
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/multi")
    populate_multi_project_repo(config, db, repo_id)

    preprocess.run(config)

    preprocess_repo_dir = config.preprocess_dir / "owner__multi"
    names = sorted(path.name for path in preprocess_repo_dir.glob("*.txt"))
    assert any(name.startswith("proj0001-") for name in names)
    assert any(name.startswith("proj0002-") for name in names)
