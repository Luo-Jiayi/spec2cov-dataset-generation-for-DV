from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Any

from spec2cov.parsing.doc_extractors import extract_terms, normalize_match_key

COVERGROUP_RE = re.compile(r"(?P<body>covergroup\b.*?endgroup)", re.IGNORECASE | re.DOTALL)
MODULE_RE = re.compile(r"(?P<body>module\b\s+(?P<name>[A-Za-z_][A-Za-z0-9_$]*)[\s\S]*?endmodule)", re.IGNORECASE)
INTERFACE_RE = re.compile(r"(?P<body>interface\b\s+(?P<name>[A-Za-z_][A-Za-z0-9_$]*)[\s\S]*?endinterface)", re.IGNORECASE)
COVERGROUP_NAME_RE = re.compile(r"covergroup\s+([A-Za-z_][A-Za-z0-9_$]*)", re.IGNORECASE)
COVERPOINT_TERM_RE = re.compile(r"coverpoint\s+([A-Za-z_][A-Za-z0-9_$]*)", re.IGNORECASE)
CROSS_TERM_RE = re.compile(r"cross\s+([^;]+);", re.IGNORECASE)
ALWAYS_BLOCK_RE = re.compile(r"always(?:_ff|_comb|_latch)?\b[\s\S]*?end", re.IGNORECASE)
INITIAL_BLOCK_RE = re.compile(r"initial\b[\s\S]*?end", re.IGNORECASE)
ASSIGN_RE = re.compile(r"^\s*assign\b.*?$", re.IGNORECASE | re.MULTILINE)
COVER_KEYWORDS = ("covergroup", "coverpoint", "bins", "cross")


def _line_span(text: str, start: int, end: int) -> dict[str, int]:
    start_line = text.count("\n", 0, start) + 1
    end_line = text.count("\n", 0, end) + 1
    return {"start_line": start_line, "end_line": end_line}


def try_parse_with_pyverilog(path: Path) -> dict[str, Any]:
    try:
        from pyverilog.vparser.parser import parse
    except Exception as exc:
        return {"parser": "pyverilog", "success": False, "fallback": True, "error": f"import_error:{exc}"}

    try:
        ast, _ = parse([str(path)])
        description = getattr(ast, "description", None)
        definitions = getattr(description, "definitions", []) if description is not None else []
        names = [getattr(node, "name", type(node).__name__) for node in definitions]
        return {"parser": "pyverilog", "success": True, "fallback": False, "definitions": names}
    except Exception as exc:
        return {"parser": "pyverilog", "success": False, "fallback": True, "error": str(exc)}


def extract_cover_keywords(text: str) -> dict[str, list[str]]:
    covergroup_names = [match.group(1) for match in COVERGROUP_NAME_RE.finditer(text)]
    coverpoint_terms = [match.group(1) for match in COVERPOINT_TERM_RE.finditer(text)]
    cross_terms: list[str] = []
    for match in CROSS_TERM_RE.finditer(text):
        cross_terms.extend(part.strip() for part in match.group(1).split(",") if part.strip())
    normalized_keywords = sorted(
        {
            normalize_match_key(value)
            for value in [*covergroup_names, *coverpoint_terms, *cross_terms]
            if normalize_match_key(value)
        }
    )
    return {
        "covergroup_names": covergroup_names,
        "coverpoint_terms": coverpoint_terms,
        "cross_terms": cross_terms,
        "normalized_keywords": normalized_keywords,
    }


def _strip_procedural_blocks(body: str) -> str:
    stripped = ALWAYS_BLOCK_RE.sub("", body)
    stripped = INITIAL_BLOCK_RE.sub("", stripped)
    stripped = ASSIGN_RE.sub("", stripped)
    return stripped


def _remove_cover_statements(body: str) -> str:
    return COVERGROUP_RE.sub("", body)


def extract_sv_cover_artifacts(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    parser_info = try_parse_with_pyverilog(path)
    results: list[dict[str, Any]] = []
    for index, match in enumerate(COVERGROUP_RE.finditer(text), start=1):
        body = match.group("body").strip()
        if not any(keyword in body.lower() for keyword in COVER_KEYWORDS):
            continue
        keyword_info = extract_cover_keywords(body)
        results.append(
            {
                "type": "cover",
                "name": keyword_info["covergroup_names"][0] if keyword_info["covergroup_names"] else f"{path.stem}-cover-{index}",
                "content": body,
                "span": _line_span(text, match.start("body"), match.end("body")),
                "parser": parser_info,
                "metadata": {"source_type": path.suffix.lower(), **keyword_info},
            }
        )
    return results


def extract_sv_dut_artifacts(path: Path, keyword_terms: set[str] | None = None) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    parser_info = try_parse_with_pyverilog(path)
    results: list[dict[str, Any]] = []

    for regex, artifact_type in ((MODULE_RE, "dut"), (INTERFACE_RE, "dut")):
        for index, match in enumerate(regex.finditer(text), start=1):
            body = match.group("body").strip()
            cleaned = _remove_cover_statements(_strip_procedural_blocks(body)).strip()
            if not cleaned:
                continue
            if keyword_terms and not (extract_terms(cleaned) & keyword_terms):
                continue
            name = match.groupdict().get("name") or f"{path.stem}-{artifact_type}-{index}"
            results.append(
                {
                    "type": artifact_type,
                    "name": name,
                    "content": cleaned,
                    "span": _line_span(text, match.start("body"), match.end("body")),
                    "parser": parser_info,
                    "metadata": {"source_type": path.suffix.lower(), "normalized_keywords": sorted(extract_terms(cleaned))[:200]},
                }
            )
    return results


def extract_sv_artifacts(path: Path) -> list[dict[str, Any]]:
    cover = extract_sv_cover_artifacts(path)
    dut = extract_sv_dut_artifacts(path)
    results = [*dut, *cover]
    if not results:
        text = path.read_text(encoding="utf-8", errors="ignore")
        parser_info = try_parse_with_pyverilog(path)
        if parser_info.get("success"):
            with tempfile.NamedTemporaryFile("w", suffix=path.suffix, delete=False, encoding="utf-8") as handle:
                handle.write(text)
                temp_path = Path(handle.name)
            try:
                parser_info = try_parse_with_pyverilog(temp_path)
            finally:
                temp_path.unlink(missing_ok=True)
    return results
