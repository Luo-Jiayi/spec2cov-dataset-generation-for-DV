from __future__ import annotations

from pathlib import Path

from spec2cov.parsing.doc_extractors import extract_terms, normalize_match_key
from spec2cov.parsing.sv_pyverilog import extract_cover_keywords, extract_sv_dut_artifacts


def test_normalize_match_key_ignores_case_and_symbols():
    assert normalize_match_key("head_ptr_f") == "headptrf"
    assert normalize_match_key("HeadPtrF") == "headptrf"
    assert normalize_match_key("head-ptr-f") == "headptrf"


def test_extract_cover_keywords_collects_covergroup_coverpoint_and_cross_terms():
    text = """
covergroup buffer_cover @ (posedge clk);
  head: coverpoint head_ptr_f;
  tail: coverpoint tail_ptr_f;
  cross head, tail;
endgroup
"""
    info = extract_cover_keywords(text)
    assert "buffer_cover" in info["covergroup_names"]
    assert "head_ptr_f" in info["coverpoint_terms"]
    assert "head" in info["cross_terms"]
    assert "buffercover" in info["normalized_keywords"]


def test_extract_sv_dut_artifacts_filters_procedural_blocks_and_matches_keywords(tmp_path: Path):
    path = tmp_path / "dut.sv"
    path.write_text(
        """
module fifo_dut(input logic clk, input logic [3:0] head_ptr_f);
  logic [3:0] tail_ptr_f;
  assign out = head_ptr_f;
  always_ff @(posedge clk) begin
    tail_ptr_f <= head_ptr_f;
  end
endmodule
""",
        encoding="utf-8",
    )

    artifacts = extract_sv_dut_artifacts(path, {"headptrf", "tailptrf"})

    assert artifacts
    content = artifacts[0]["content"]
    assert "assign out" not in content
    assert "always_ff" not in content
    assert "head_ptr_f" in content
