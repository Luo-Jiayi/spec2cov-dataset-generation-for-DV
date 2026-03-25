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
        lambda text, path, keyword_terms, parser_info: (
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
