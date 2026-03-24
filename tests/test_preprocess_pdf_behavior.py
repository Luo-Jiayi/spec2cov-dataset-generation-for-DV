from __future__ import annotations

from pathlib import Path

from spec2cov.config import AppConfig, ExportConfig
from spec2cov.db.schema import create_all
from spec2cov.db.repository import Database
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
    repo_id = db.upsert_repository(
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
    return repo_id


def test_preprocess_generates_placeholder_spec_and_keeps_repo(tmp_path: Path):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/repo")
    repo_dir = config.raw_dir / "owner__repo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    (repo_dir / "tb.sv").write_text("module dut(input logic a); endmodule\ncovergroup cg; coverpoint a; endgroup", encoding="utf-8")
    (repo_dir / "notes.md").write_text("See details in design.pdf", encoding="utf-8")

    db.upsert_candidate_file(repo_id, {"path": "tb.sv", "ext": ".sv", "size_bytes": 10, "source_url": "", "commit_sha": "sha1", "metadata": {}})
    db.upsert_candidate_file(repo_id, {"path": "notes.md", "ext": ".md", "size_bytes": 10, "source_url": "", "commit_sha": "sha2", "metadata": {}})

    run_id = preprocess.run(config)

    assert run_id > 0
    artifacts = db.list_artifacts(repo_id=repo_id)
    spec_artifacts = [row for row in artifacts if row["artifact_type"] == "spec"]
    assert spec_artifacts
    paths = [Path(row["content_path"]) for row in spec_artifacts]
    assert any(path.read_text(encoding="utf-8") == "" for path in paths)


def test_preprocess_pdf_spec_extraction_is_saved(tmp_path: Path):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/pdfrepo")
    repo_dir = config.raw_dir / "owner__pdfrepo"
    repo_dir.mkdir(parents=True, exist_ok=True)

    (repo_dir / "tb.sv").write_text("module dut(input logic a); endmodule\ncovergroup cg; coverpoint a; endgroup", encoding="utf-8")
    (repo_dir / "plan.xml").write_text("<root><row><field>mode</field><value>auto</value></row></root>", encoding="utf-8")
    pdf_path = repo_dir / "spec.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%mock")

    db.upsert_candidate_file(repo_id, {"path": "tb.sv", "ext": ".sv", "size_bytes": 10, "source_url": "", "commit_sha": "sha1", "metadata": {}})
    db.upsert_candidate_file(repo_id, {"path": "plan.xml", "ext": ".xml", "size_bytes": 10, "source_url": "", "commit_sha": "sha2", "metadata": {}})
    db.upsert_candidate_file(repo_id, {"path": "spec.pdf", "ext": ".pdf", "size_bytes": 10, "source_url": "", "commit_sha": "sha3", "metadata": {}})

    import spec2cov.parsing.doc_extractors as extractors

    class DummyPage:
        def __init__(self, text: str):
            self._text = text

        def extract_text(self) -> str:
            return self._text

    class DummyReader:
        def __init__(self, _path: str):
            self.pages = [DummyPage("The signal a is covered in the PDF spec content")]

    original = extractors.PdfReader
    extractors.PdfReader = DummyReader
    try:
        preprocess.run(config)
    finally:
        extractors.PdfReader = original

    artifacts = db.list_artifacts(repo_id=repo_id)
    plan_paths = [Path(row["content_path"]) for row in artifacts if row["artifact_type"] == "plan"]
    spec_paths = [Path(row["content_path"]) for row in artifacts if row["artifact_type"] == "spec"]
    assert any("| path | value |" in path.read_text(encoding="utf-8") for path in plan_paths)
    assert any("## Page 1" in path.read_text(encoding="utf-8") for path in spec_paths)
