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
    names = sorted(path.name for path in preprocess_repo_dir.glob("*"))
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
    names = sorted(path.name for path in preprocess_repo_dir.glob("*"))
    assert any(name.startswith("proj0001-") for name in names)
    assert any(name.startswith("proj0002-") for name in names)


def test_preprocess_uses_cover_names_for_spec_but_only_signal_names_for_dut(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    import spec2cov.parsing.sv_pyverilog as sv_module

    monkeypatch.setattr(
        sv_module,
        "try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/split")
    repo_dir = config.raw_dir / "owner__split"
    repo_dir.mkdir(parents=True, exist_ok=True)

    (repo_dir / "tb.sv").write_text(
        "covergroup buffer_cover; cp_head: coverpoint fifo_if.head_ptr_f; endgroup\n"
        "module cp_head_wrapper(input logic clk); endmodule\n"
        "module fifo_dut(input logic head_ptr_f); endmodule\n",
        encoding="utf-8",
    )
    (repo_dir / "spec.md").write_text("The cp_head item belongs to buffer_cover behavior.", encoding="utf-8")

    db.upsert_candidate_file(repo_id, {"path": "tb.sv", "ext": ".sv", "size_bytes": 10, "source_url": "", "commit_sha": "1", "metadata": {}})
    db.upsert_candidate_file(repo_id, {"path": "spec.md", "ext": ".md", "size_bytes": 10, "source_url": "", "commit_sha": "2", "metadata": {}})

    preprocess.run(config)

    preprocess_repo_dir = config.preprocess_dir / "owner__split"
    spec_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-spec*.txt")]
    dut_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-dut*.sv")]

    assert spec_contents
    assert any("buffer_cover" in content or "cp_head" in content for content in spec_contents)
    assert any("fifo_dut" in content for content in dut_contents)
    assert all("cp_head_wrapper" not in content for content in dut_contents)


def test_preprocess_uses_same_file_dut_for_cover_property_files(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    import spec2cov.parsing.sv_pyverilog as sv_module

    monkeypatch.setattr(
        sv_module,
        "try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/coverprop")
    repo_dir = config.raw_dir / "owner__coverprop"
    repo_dir.mkdir(parents=True, exist_ok=True)

    (repo_dir / "tb.sv").write_text(
        "property p_ready;\n"
        "  @(posedge clk) req |-> ready;\n"
        "endproperty\n"
        "cp_ready: cover property(p_ready);\n"
        "module dut_only_here(input logic req, input logic ready);\n"
        "endmodule\n",
        encoding="utf-8",
    )

    db.upsert_candidate_file(repo_id, {"path": "tb.sv", "ext": ".sv", "size_bytes": 10, "source_url": "", "commit_sha": "1", "metadata": {}})

    preprocess.run(config)

    preprocess_repo_dir = config.preprocess_dir / "owner__coverprop"
    cover_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-cover*.sv")]
    dut_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-dut*.sv")]

    assert any("cover property(p_ready)" in content for content in cover_contents)
    assert any("module dut_only_here" in content for content in dut_contents)


def test_preprocess_removes_assert_property_from_same_interface_dut(tmp_path: Path, monkeypatch):
    config = make_config(tmp_path)
    for path in [config.data_root, config.raw_dir, config.preprocess_dir, config.export_dir, config.log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    import spec2cov.parsing.sv_pyverilog as sv_module

    monkeypatch.setattr(
        sv_module,
        "try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    db = Database(str(config.db_path))
    create_all(db.engine)
    repo_id = seed_repo(db, "owner/ubus")
    repo_dir = config.raw_dir / "owner__ubus"
    repo_dir.mkdir(parents=True, exist_ok=True)

    (repo_dir / "ubus_if.sv").write_text(
        "interface ubus_if(input logic clk);\n"
        "  logic sig_addr;\n"
        "  logic has_checks;\n"
        "  assertAddrUnknown: assert property (\n"
        "    @(posedge clk) disable iff(!has_checks) !$isunknown(sig_addr)\n"
        "  );\n"
        "  cp_addr_ok: cover property (@(posedge clk) sig_addr == sig_addr);\n"
        "  modport mon(input sig_addr);\n"
        "endinterface\n",
        encoding="utf-8",
    )

    db.upsert_candidate_file(repo_id, {"path": "ubus_if.sv", "ext": ".sv", "size_bytes": 10, "source_url": "", "commit_sha": "1", "metadata": {}})

    preprocess.run(config)

    preprocess_repo_dir = config.preprocess_dir / "owner__ubus"
    assert_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-assert*.sv")]
    cover_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-cover*.sv")]
    dut_contents = [path.read_text(encoding="utf-8") for path in preprocess_repo_dir.glob("*-dut*.sv")]

    assert any("assertAddrUnknown: assert property" in content for content in assert_contents)
    assert any("cover property" in content for content in cover_contents)
    assert any("interface ubus_if" in content for content in dut_contents)
    assert all("assertAddrUnknown" not in content for content in dut_contents)
    assert all("assert property" not in content for content in dut_contents)
    assert all("cover property" not in content for content in dut_contents)
    assert all("property " not in content for content in dut_contents)


def test_assign_project_indices_duplicates_dut_across_equal_keyword_clusters(tmp_path: Path):
    config = make_config(tmp_path)
    config.filters.project_cluster_file_threshold = 1

    extracted = [
        (
            {
                "type": "cover",
                "name": "cover_a",
                "content": "covergroup cover_a; cp_a: coverpoint if_a.shared_sig; endgroup",
                "metadata": {
                    "source_rel_path": "projA/tb_a.sv",
                    "normalized_keywords": ["covera", "sharedsig"],
                    "matched_terms": [],
                },
            },
            1,
        ),
        (
            {
                "type": "cover",
                "name": "cover_b",
                "content": "covergroup cover_b; cp_b: coverpoint if_b.shared_sig; endgroup",
                "metadata": {
                    "source_rel_path": "projB/tb_b.sv",
                    "normalized_keywords": ["coverb", "sharedsig"],
                    "matched_terms": [],
                },
            },
            2,
        ),
        (
            {
                "type": "dut",
                "name": "shared_dut",
                "content": "module shared_dut(input logic shared_sig); endmodule",
                "metadata": {
                    "source_rel_path": "",
                    "normalized_keywords": ["shareddut", "sharedsig"],
                    "matched_terms": ["sharedsig"],
                },
            },
            3,
        ),
    ]
    repo_files = {"projA/tb_a.sv": {"file_id": 1}, "projB/tb_b.sv": {"file_id": 2}}

    assigned = preprocess.assign_project_indices(extracted, repo_files, config)
    dut_assignments = [(index, artifact) for index, artifact, _ in assigned if artifact["type"] == "dut"]

    assert len(dut_assignments) == 2
    assert {index for index, _ in dut_assignments} == {1, 2}
    assert all(artifact["metadata"]["project_index"] in {1, 2} for _, artifact in dut_assignments)
