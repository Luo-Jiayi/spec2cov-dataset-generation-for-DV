from spec2cov.config import FilterConfig
from spec2cov.filtering.keyword_filter import has_minimum_text, should_keep_sv_file


def test_should_keep_sv_file_when_keywords_present():
    keep, hits = should_keep_sv_file("module dut; covergroup cg; coverpoint a; endgroup endmodule", FilterConfig())
    assert keep is True
    assert hits["module"] >= 1
    assert hits["covergroup"] >= 1


def test_should_reject_sv_file_without_module_or_interface():
    keep, _ = should_keep_sv_file("class my_test; rand int a; endclass", FilterConfig())
    assert keep is False


def test_has_minimum_text():
    assert has_minimum_text("abc", 2) is True
    assert has_minimum_text(" ", 1) is False
