from spec2cov.config import DedupConfig
from spec2cov.filtering.dedup import compare_against_existing, sha256_text


def test_sha256_text_is_stable():
    assert sha256_text("hello") == sha256_text("hello")


def test_compare_against_existing_detects_exact_duplicate():
    config = DedupConfig(jaccard_threshold=0.8)
    existing = [{"text": "module a; covergroup cg; endgroup endmodule", "content_sha256": sha256_text("module a; covergroup cg; endgroup endmodule"), "cluster_id": "abc"}]
    result = compare_against_existing("module a; covergroup cg; endgroup endmodule", existing, config)
    assert result.near_duplicate is True
    assert result.similarity == 1.0


def test_compare_against_existing_accepts_distinct_text():
    config = DedupConfig(jaccard_threshold=0.95)
    existing = [{"text": "module alpha; logic a; endmodule", "content_sha256": sha256_text("module alpha; logic a; endmodule"), "cluster_id": "abc"}]
    result = compare_against_existing("interface beta; logic clk; endinterface", existing, config)
    assert result.near_duplicate is False
