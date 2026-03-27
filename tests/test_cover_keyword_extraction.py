from __future__ import annotations

from pathlib import Path

from spec2cov.parsing.doc_extractors import normalize_match_key
from spec2cov.parsing.sv_pyverilog import _strip_procedural_blocks, extract_cover_keywords, extract_sv_cover_artifacts, extract_sv_dut_artifacts


def test_normalize_match_key_ignores_case_and_symbols():
    assert normalize_match_key("head_ptr_f") == "headptrf"
    assert normalize_match_key("HeadPtrF") == "headptrf"
    assert normalize_match_key("head-ptr-f") == "headptrf"


def test_extract_cover_keywords_collects_covergroup_coverpoint_and_cross_terms():
    text = """
covergroup buffer_cover @ (posedge clk);
  head: coverpoint fifo_if.head_ptr_f;
  tail: coverpoint fifo_if.tail_ptr_f;
  cross head, tail;
endgroup
"""
    info = extract_cover_keywords(text)
    assert "buffer_cover" in info["covergroup_names"]
    assert "head" in info["coverpoint_names"]
    assert "fifo_if.head_ptr_f" in info["coverpoint_targets"]
    assert "head_ptr_f" in info["coverpoint_signal_terms"]
    assert "head" in info["cross_terms"]
    assert "buffercover" in info["normalized_keywords"]
    assert "fifoifheadptrf" in info["normalized_keywords"]


def test_extract_cover_keywords_keeps_coverpoint_name_separate_from_target_signal():
    text = "cp_name: coverpoint dut_if.sig_name;"

    info = extract_cover_keywords(text)

    assert info["coverpoint_names"] == ["cp_name"]
    assert info["coverpoint_targets"] == ["dut_if.sig_name"]
    assert info["coverpoint_signal_terms"] == ["sig_name"]


def test_extract_sv_cover_artifacts_keeps_regex_covergroup_extraction(tmp_path: Path):
    path = tmp_path / "cover.sv"
    path.write_text(
        """
covergroup buffer_cover @ (posedge clk);
  head: coverpoint fifo_if.head_ptr_f;
endgroup
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)

    assert len(artifacts) == 1
    assert artifacts[0]["name"] == "buffer_cover"
    assert "coverpoint fifo_if.head_ptr_f" in artifacts[0]["content"]


def test_extract_sv_cover_artifacts_supports_named_endgroup_suffix(tmp_path: Path):
    path = tmp_path / "cover_named_end.sv"
    path.write_text(
        """
covergroup buffer_cover @ (posedge clk);
  head: coverpoint fifo_if.head_ptr_f;
endgroup : buffer_cover;
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)

    assert len(artifacts) == 1
    assert artifacts[0]["content"].strip().endswith("endgroup : buffer_cover;")


def test_extract_sv_cover_artifacts_ignores_covergroup_keywords_inside_comments(tmp_path: Path):
    path = tmp_path / "cover_comment.sv"
    path.write_text(
        """
// covergroup bogus_comment;
covergroup real_cover;
  cp: coverpoint dut_if.sig;
endgroup
""",
        encoding="utf-8",
    )

    artifacts = [artifact for artifact in extract_sv_cover_artifacts(path) if artifact["type"] == "cover"]

    assert len(artifacts) == 1
    assert artifacts[0]["name"] == "real_cover"


def test_extract_sv_cover_artifacts_ignores_stray_covergroup_token_before_real_definition(tmp_path: Path):
    path = tmp_path / "cover_prefix_bug.sv"
    path.write_text(
        """
covergroup
  covergroup cov_trans @cov_transaction;
    trans_start_addr : coverpoint trans_collected.addr;
  endgroup : cov_trans
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)

    assert len(artifacts) == 1
    assert artifacts[0]["name"] == "cov_trans"
    assert artifacts[0]["content"].lstrip().startswith("covergroup cov_trans")
    assert not artifacts[0]["content"].lstrip().startswith("covergroup\n  covergroup")


def test_extract_sv_cover_artifacts_extracts_cover_property_with_named_property_block(tmp_path: Path):
    path = tmp_path / "cover_property.sv"
    path.write_text(
        """
property p_addr_ok;
  @(posedge clk) req |-> ack;
endproperty
cp_addr_ok: cover property(p_addr_ok);
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)
    cover_artifacts = [artifact for artifact in artifacts if artifact["type"] == "cover" and artifact["metadata"].get("statement_kind") == "cover_property"]

    assert len(cover_artifacts) == 1
    assert cover_artifacts[0]["name"] == "cp_addr_ok"
    assert "property p_addr_ok;" in cover_artifacts[0]["content"]
    assert "cp_addr_ok: cover property(p_addr_ok);" in cover_artifacts[0]["content"]
    assert cover_artifacts[0]["metadata"]["same_file_dut"] is True


def test_extract_sv_cover_artifacts_extracts_named_property_header_as_part_of_cover_property_block(tmp_path: Path):
    path = tmp_path / "cover_property_args.sv"
    path.write_text(
        """
property p_addr_ok(bit sig);
  @(posedge clk) !$isunknown(sig);
endproperty
cp_addr_ok: cover property(p_addr_ok);
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)
    cover_artifacts = [artifact for artifact in artifacts if artifact["type"] == "cover" and artifact["metadata"].get("statement_kind") == "cover_property"]

    assert len(cover_artifacts) == 1
    assert "property p_addr_ok(bit sig);" in cover_artifacts[0]["content"]
    assert "endproperty" in cover_artifacts[0]["content"]


def test_extract_sv_cover_artifacts_extracts_assert_property_as_assert_artifact(tmp_path: Path):
    path = tmp_path / "assert_property.sv"
    path.write_text(
        """
assert_addr_ok: assert property (@(posedge clk) req |-> ack);
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_cover_artifacts(path)
    assert_artifacts = [artifact for artifact in artifacts if artifact["type"] == "assert"]

    assert len(assert_artifacts) == 1
    assert assert_artifacts[0]["name"] == "assert_addr_ok"
    assert "assert property" in assert_artifacts[0]["content"]
    assert assert_artifacts[0]["metadata"]["statement_kind"] == "assert_property"


def test_extract_sv_cover_artifacts_stops_assert_extraction_before_next_property_declaration_without_semicolon(tmp_path: Path):
    path = tmp_path / "assert_no_semicolon.sv"
    path.write_text(
        """
property p_penable_rose_next_cycle_fall;
  @(posedge clk) penable && pready |=> $fell(penable);
endproperty: p_penable_rose_next_cycle_fall
assert property(p_penable_rose_next_cycle_fall) else `uvm_error("ASSERT", "PENABLE not fall after 1 cycle PENABLE rose")

property p_pwdata_stable_during_trans_phase;
  @(posedge clk) ((psel && !penable) ##1 (psel && penable)) |-> $stable(pwdata);
endproperty: p_pwdata_stable_during_trans_phase
assert property(p_pwdata_stable_during_trans_phase) else `uvm_error("ASSERT", "PWDATA not stable during transaction phase")
""",
        encoding="utf-8",
    )

    artifacts = [artifact for artifact in extract_sv_cover_artifacts(path) if artifact["type"] == "assert"]

    assert len(artifacts) == 2
    assert "property p_pwdata_stable_during_trans_phase;" not in artifacts[0]["content"]
    assert "property p_pwdata_stable_during_trans_phase;" in artifacts[1]["content"]


def test_strip_procedural_blocks_deletes_lines_instead_of_leaving_blank_lines():
    body = (
        "module fifo_dut(input logic clk);\n"
        "  logic keep_a;\n"
        "  assign out = keep_a;\n"
        "  always_ff @(posedge clk) begin\n"
        "    keep_a <= 1'b1;\n"
        "  end\n"
        "  logic keep_b;\n"
        "endmodule\n"
    )

    stripped = _strip_procedural_blocks(body)

    assert "assign out" not in stripped
    assert "always_ff" not in stripped
    assert "keep_a <= 1'b1;" not in stripped
    assert "\n\n" not in stripped
    assert "  logic keep_a;\n  logic keep_b;" in stripped


def test_strip_procedural_blocks_removes_nested_initial_block_without_leaving_orphan_end(tmp_path: Path):
    body = (
        "interface kei_vip_apb_if(input logic clk);\n"
        "  logic paddr;\n"
        "  initial begin: assertion_control\n"
        "    fork\n"
        "      forever begin\n"
        "        wait(clk == 0);\n"
        "        wait(clk == 1);\n"
        "      end\n"
        "    join_none\n"
        "  end\n"
        "  modport mon(input paddr);\n"
        "endinterface\n"
    )

    stripped = _strip_procedural_blocks(body)

    assert "initial begin: assertion_control" not in stripped
    assert "join_none" not in stripped
    assert "forever begin" not in stripped
    assert "  modport mon(input paddr);" in stripped
    assert "  end\n  modport" not in stripped


def test_extract_sv_dut_artifacts_regex_fallback_keeps_complete_module_without_procedural_fragments(tmp_path: Path, monkeypatch):
    path = tmp_path / "dut.sv"
    path.write_text(
        """
module fifo_dut(input logic clk, input logic [3:0] head_ptr_f);
  logic [3:0] tail_ptr_f;
  assign out = head_ptr_f;
  always_ff @(posedge clk) begin
    tail_ptr_f <= head_ptr_f;
  end
  logic keep_me;
endmodule
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"headptrf", "tailptrf"})

    assert artifacts
    content = artifacts[0]["content"]
    metadata = artifacts[0]["metadata"]
    assert "assign out" not in content
    assert "always_ff" not in content
    assert "head_ptr_f" in content
    assert "tail_ptr_f <= head_ptr_f" not in content
    assert "begin" not in content
    assert content.strip().endswith("endmodule")
    assert metadata["extraction_mode"] == "regex_module"
    assert metadata["fallback_reason"] == "unsupported_sv"
    assert metadata["matched_terms"] == ["headptrf", "tailptrf"]


def test_extract_sv_dut_artifacts_ast_path_is_preferred_when_available(tmp_path: Path, monkeypatch):
    path = tmp_path / "dut.v"
    path.write_text("module fifo_dut(input clk); endmodule\n", encoding="utf-8")

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": True, "fallback": False, "definitions": ["fifo_dut"]},
    )
    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog._extract_ast_module_artifacts",
        lambda text, path, keyword_terms, parser_info, include_all=False: (
            [
                {
                    "type": "dut",
                    "name": "fifo_dut",
                    "content": "module fifo_dut(\n  input clk,\n  input [3:0] head_ptr_f\n);\n\n  logic [3:0] tail_ptr_f;\nendmodule",
                    "span": {"start_line": 1, "end_line": 1},
                    "parser": parser_info,
                    "metadata": {
                        "source_type": ".v",
                        "normalized_keywords": ["fifo", "headptrf", "tailptrf"],
                        "matched_terms": ["headptrf", "tailptrf"],
                        "selected_definition_names": ["fifo_dut"],
                        "extraction_mode": "ast_module",
                        "parse_success": True,
                        "fallback_reason": None,
                    },
                }
            ],
            None,
        ),
    )

    artifacts = extract_sv_dut_artifacts(path, {"headptrf", "tailptrf"})

    assert len(artifacts) == 1
    assert artifacts[0]["metadata"]["extraction_mode"] == "ast_module"
    assert artifacts[0]["metadata"]["parse_success"] is True
    assert "tail_ptr_f" in artifacts[0]["content"]


def test_extract_sv_dut_artifacts_extracts_complete_interface_via_regex(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface fifo_if(input logic clk);
  logic head_ptr_f;
  modport mon(input head_ptr_f);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"headptrf"})

    assert len(artifacts) == 1
    assert artifacts[0]["content"].strip().endswith("endinterface")
    assert artifacts[0]["metadata"]["extraction_mode"] == "regex_interface"
    assert artifacts[0]["metadata"]["fallback_reason"] == "systemverilog_interface_regex_only"


def test_extract_sv_dut_artifacts_removes_assert_property_from_interface(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface ubus_if(input logic clk);
  logic sig_addr;
  logic has_checks;
  assertAddrUnknown: assert property (@(posedge clk) disable iff(!has_checks) !$isunknown(sig_addr));
  modport mon(input sig_addr);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"sigaddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "assertAddrUnknown" not in content
    assert "assert property" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_removes_named_property_and_cover_property_from_interface(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface ubus_if(input logic clk);
  logic sig_addr;
  property p_addr_ok;
    @(posedge clk) !$isunknown(sig_addr);
  endproperty
  cp_addr_ok: cover property(p_addr_ok);
  modport mon(input sig_addr);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"sigaddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "property p_addr_ok" not in content
    assert "cover property" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_removes_named_property_header_line_from_interface(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface ubus_if(input logic clk);
  logic sig_addr;
  property p_addr_ok(bit sig);
    @(posedge clk) !$isunknown(sig);
  endproperty
  cp_addr_ok: cover property(p_addr_ok);
  modport mon(input sig_addr);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"sigaddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "property p_addr_ok(bit sig);" not in content
    assert "endproperty" not in content
    assert "cover property" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_removes_unreferenced_property_block_from_interface(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface kei_vip_apb_if(input logic clk);
  logic prdata;
  logic penable;
  logic pwrite;
  logic pready;
  property p_prdata_available_once_penable_rose;
    @(posedge clk) penable && !pwrite && pready |-> !$stable(prdata);
  endproperty: p_prdata_available_once_penable_rose
  //assert property(p_prdata_available_once_penable_rose) else `uvm_error("ASSERT", "PRDATA not available once PENABLE rose")
  modport mon(input prdata);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"prdata"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "property p_prdata_available_once_penable_rose" not in content
    assert "@(posedge clk) penable && !pwrite && pready |-> !$stable(prdata);" not in content
    assert "endproperty: p_prdata_available_once_penable_rose" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_interface_uses_shared_cleanup_for_covergroup_initial_and_property(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface kei_vip_apb_if(input logic pclk);
  logic paddr;
  covergroup cg_apb;
    cp_addr: coverpoint paddr;
  endgroup
  initial begin
    paddr = '0;
  end
  property p_addr_ok;
    @(posedge pclk) !$isunknown(paddr);
  endproperty
  apb_addr_cov: cover property(p_addr_ok);
  modport mon(input paddr);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"paddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "covergroup" not in content
    assert "endgroup" not in content
    assert "initial begin" not in content
    assert "property p_addr_ok" not in content
    assert "cover property" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_interface_removes_nested_initial_block_without_orphan_end(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
interface kei_vip_apb_if(input logic clk);
  logic paddr;
  initial begin: assertion_control
    fork
      forever begin
        wait(clk == 0);
        wait(clk == 1);
      end
    join_none
  end
  modport mon(input paddr);
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"paddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "initial begin: assertion_control" not in content
    assert "join_none" not in content
    assert "forever begin" not in content
    assert "  end\n  modport" not in content
    assert "modport mon" in content


def test_extract_sv_dut_artifacts_regex_module_removes_named_property_and_assert_statement(tmp_path: Path, monkeypatch):
    path = tmp_path / "dut.sv"
    path.write_text(
        """
module fifo_dut(input logic clk, input logic sig_addr);
  logic keep_me;
  property p_addr_ok;
    @(posedge clk) !$isunknown(sig_addr);
  endproperty
  assert_addr_ok: assert property(p_addr_ok);
endmodule
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, {"sigaddr"})

    assert len(artifacts) == 1
    content = artifacts[0]["content"]
    assert "property p_addr_ok" not in content
    assert "assert property" not in content
    assert "logic keep_me;" in content


def test_extract_sv_dut_artifacts_ignores_interface_keyword_inside_comments(tmp_path: Path, monkeypatch):
    path = tmp_path / "tb_if.sv"
    path.write_text(
        """
// interface which is a comment, not a declaration
interface AXI_vif(input logic clk);
  logic head_ptr_f;
endinterface
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    artifacts = extract_sv_dut_artifacts(path, set(), include_all=True)

    assert len(artifacts) == 1
    assert artifacts[0]["name"] == "AXI_vif"
    assert "interface AXI_vif" in artifacts[0]["content"]


def test_extract_sv_dut_artifacts_returns_nothing_without_matching_terms(tmp_path: Path, monkeypatch):
    path = tmp_path / "dut.sv"
    path.write_text("module fifo_dut(input logic clk); logic idle; endmodule\n", encoding="utf-8")
    monkeypatch.setattr(
        "spec2cov.parsing.sv_pyverilog.try_parse_with_pyverilog",
        lambda _: {"parser": "pyverilog", "success": False, "fallback": True, "error": "unsupported_sv"},
    )

    assert extract_sv_dut_artifacts(path, {"headptrf"}) == []


def test_extract_sv_dut_artifacts_returns_nothing_when_keyword_terms_missing(tmp_path: Path):
    path = tmp_path / "dut.sv"
    path.write_text("module fifo_dut(input logic clk); logic idle; endmodule\n", encoding="utf-8")

    assert extract_sv_dut_artifacts(path, None) == []
