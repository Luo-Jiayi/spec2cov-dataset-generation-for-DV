from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from spec2cov.parsing.doc_extractors import (
    build_dut_keyword_terms,
    build_spec_keyword_terms,
    extract_markdown_pdf_reference_lines,
    extract_pdf_spec,
    extract_xlsx_plan,
    extract_xml_plan,
    markdown_mentions_pdf,
)


def test_extract_xml_plan_outputs_markdown_table(tmp_path: Path):
    path = tmp_path / "plan.xml"
    path.write_text("<root><item><name>mode</name><value>auto</value></item></root>", encoding="utf-8")

    artifacts = extract_xml_plan(path)

    assert artifacts
    content = artifacts[0]["content"]
    assert "| path | value |" in content
    assert "| --- | --- |" in content


def test_extract_xlsx_plan_outputs_markdown_table(tmp_path: Path):
    path = tmp_path / "plan.xlsx"
    with ZipFile(path, "w", ZIP_DEFLATED) as zf:
        pass

    import openpyxl

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Plan"
    sheet.append(["feature", "value"])
    sheet.append(["burst", "on"])
    workbook.save(path)

    artifacts = extract_xlsx_plan(path)

    assert artifacts
    content = artifacts[0]["content"]
    assert "# Sheet: Plan" in content
    assert "| feature | value |" in content
    assert "| --- | --- |" in content
    assert "| burst | on |" in content


def test_markdown_mentions_pdf_detects_references(tmp_path: Path):
    path = tmp_path / "spec.md"
    path.write_text("See [datasheet](docs/spec.pdf) for details.", encoding="utf-8")

    assert markdown_mentions_pdf(path) is True


def test_extract_markdown_pdf_reference_lines_returns_matching_lines(tmp_path: Path):
    path = tmp_path / "spec.md"
    path.write_text(
        "Intro\n"
        "See [datasheet](docs/spec.pdf) for details.\n"
        "Mirror: https://example.com/spec.pdf\n"
        "See [datasheet](docs/spec.pdf) for details.\n",
        encoding="utf-8",
    )

    assert extract_markdown_pdf_reference_lines(path) == [
        "See [datasheet](docs/spec.pdf) for details.",
        "Mirror: https://example.com/spec.pdf",
    ]


def test_extract_pdf_spec_returns_markdown_sections(tmp_path: Path):
    path = tmp_path / "spec.pdf"
    path.write_bytes(b"%PDF-1.4\n%mock")

    class DummyPage:
        def __init__(self, text: str):
            self._text = text

        def extract_text(self) -> str:
            return self._text

    class DummyReader:
        def __init__(self, _path: str):
            self.pages = [DummyPage("First page content"), DummyPage("Second page content")]

    import spec2cov.parsing.doc_extractors as module

    original = module.PdfReader
    module.PdfReader = DummyReader
    try:
        artifacts = extract_pdf_spec(path)
    finally:
        module.PdfReader = original

    assert artifacts
    content = artifacts[0]["content"]
    assert "## Page 1" in content
    assert "First page content" in content
    assert artifacts[0]["metadata"]["source_type"] == "pdf"


def test_build_spec_keyword_terms_includes_cover_names_targets_and_signal_terms():
    artifacts = [
        {
            "type": "cover",
            "content": "covergroup buffer_cover; cp_name: coverpoint dut_if.sig_name; bins sig_bin = {1}; endgroup",
            "metadata": {
                "normalized_keywords": ["buffercover", "cpname", "dutifsigname", "signame", "sigbin"],
                "covergroup_names": ["buffer_cover"],
                "coverpoint_names": ["cp_name"],
                "coverpoint_targets": ["dut_if.sig_name"],
                "coverpoint_signal_terms": ["sig_name"],
                "cross_terms": [],
            },
        }
    ]

    keywords = build_spec_keyword_terms(artifacts)

    assert "buffercover" in keywords
    assert "cpname" in keywords
    assert "dutifsigname" in keywords
    assert "signame" in keywords
    assert "sigbin" in keywords


def test_build_dut_keyword_terms_only_includes_coverpoint_signal_names():
    artifacts = [
        {
            "type": "cover",
            "content": "covergroup buffer_cover; cp_name: coverpoint dut_if.sig_name; bins sig_bin = {1}; endgroup",
            "metadata": {
                "normalized_keywords": ["buffercover", "cpname", "dutifsigname", "signame", "sigbin"],
                "covergroup_names": ["buffer_cover"],
                "coverpoint_names": ["cp_name"],
                "coverpoint_targets": ["dut_if.sig_name"],
                "coverpoint_signal_terms": ["sig_name"],
                "cross_terms": [],
            },
        }
    ]

    keywords = build_dut_keyword_terms(artifacts)

    assert keywords == {"signame"}
