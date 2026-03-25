from __future__ import annotations

import copy
import re
import tempfile
from pathlib import Path
from typing import Any

from spec2cov.parsing.doc_extractors import extract_terms, normalize_match_key

COVERGROUP_RE = re.compile(
    r"(?P<body>covergroup\b.*?endgroup(?:\s*:\s*[A-Za-z_][A-Za-z0-9_$]*)?\s*;?)",
    re.IGNORECASE | re.DOTALL,
)
MODULE_RE = re.compile(r"(?P<body>module\b\s+(?P<name>[A-Za-z_][A-Za-z0-9_$]*)[\s\S]*?endmodule)", re.IGNORECASE)
INTERFACE_RE = re.compile(r"(?P<body>interface\b\s+(?P<name>[A-Za-z_][A-Za-z0-9_$]*)[\s\S]*?endinterface)", re.IGNORECASE)
COVERGROUP_NAME_RE = re.compile(r"covergroup\s+([A-Za-z_][A-Za-z0-9_$]*)", re.IGNORECASE)
COVERPOINT_RE = re.compile(
    r"(?:(?P<label>[A-Za-z_][A-Za-z0-9_$]*)\s*:\s*)?coverpoint\s+(?P<target>[A-Za-z_][A-Za-z0-9_$.]*)",
    re.IGNORECASE,
)
CROSS_TERM_RE = re.compile(r"cross\s+([^;]+);", re.IGNORECASE)
ALWAYS_BLOCK_RE = re.compile(r"always(?:_ff|_comb|_latch)?\b[\s\S]*?end", re.IGNORECASE)
INITIAL_BLOCK_RE = re.compile(r"initial\b[\s\S]*?end", re.IGNORECASE)
ASSIGN_RE = re.compile(r"^\s*assign\b.*?$", re.IGNORECASE | re.MULTILINE)
COVER_KEYWORDS = ("covergroup", "coverpoint", "bins", "cross")
DECL_START_RE = re.compile(
    r"^\s*(?:parameter|localparam|input|output|inout|wire|logic|reg|tri|supply0|supply1|uwire|wand|wor|trireg|integer|time|realtime|real|shortreal|shortint|int|longint|byte|bit|genvar|typedef|struct|enum|union|const)\b",
    re.IGNORECASE,
)


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
    coverpoint_names: list[str] = []
    coverpoint_targets: list[str] = []
    coverpoint_signal_terms: list[str] = []
    for match in COVERPOINT_RE.finditer(text):
        label = match.group("label")
        target = match.group("target")
        if label:
            coverpoint_names.append(label)
        if target:
            coverpoint_targets.append(target)
            signal_name = target.split(".")[-1].strip()
            if signal_name:
                coverpoint_signal_terms.append(signal_name)
    cross_terms: list[str] = []
    for match in CROSS_TERM_RE.finditer(text):
        cross_terms.extend(part.strip() for part in match.group(1).split(",") if part.strip())
    normalized_keywords = sorted(
        {
            normalize_match_key(value)
            for value in [*covergroup_names, *coverpoint_names, *coverpoint_targets, *coverpoint_signal_terms, *cross_terms]
            if normalize_match_key(value)
        }
    )
    return {
        "covergroup_names": covergroup_names,
        "coverpoint_names": coverpoint_names,
        "coverpoint_targets": coverpoint_targets,
        "coverpoint_signal_terms": coverpoint_signal_terms,
        "cross_terms": cross_terms,
        "normalized_keywords": normalized_keywords,
    }


def _delete_matching_line_ranges(text: str, patterns: tuple[re.Pattern[str], ...]) -> str:
    if not text:
        return text

    line_starts = [0]
    for index, char in enumerate(text):
        if char == "\n":
            line_starts.append(index + 1)

    spans: list[tuple[int, int]] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            start = text.rfind("\n", 0, match.start()) + 1
            end_newline = text.find("\n", match.end())
            end = len(text) if end_newline == -1 else end_newline + 1
            spans.append((start, end))

    if not spans:
        return text

    spans.sort()
    merged: list[list[int]] = []
    for start, end in spans:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    parts: list[str] = []
    cursor = 0
    for start, end in merged:
        if cursor < start:
            parts.append(text[cursor:start])
        cursor = end
    if cursor < len(text):
        parts.append(text[cursor:])
    return "".join(parts)


def _strip_procedural_blocks(body: str) -> str:
    return _delete_matching_line_ranges(body, (ALWAYS_BLOCK_RE, INITIAL_BLOCK_RE, ASSIGN_RE))


def _remove_cover_statements(body: str) -> str:
    return COVERGROUP_RE.sub("", body)


def _first_line(text: str) -> str:
    return text.strip().splitlines()[0].strip() if text.strip() else ""


def _safe_codegen(node: Any, codegen: Any) -> str:
    try:
        return str(codegen.visit(node))
    except Exception:
        return ""


def _is_decl_like_item(item: Any) -> bool:
    return type(item).__name__ in {"Decl"}


def _ast_module_matches_terms(module: Any, keyword_terms: set[str], codegen: Any) -> set[str]:
    matched: set[str] = set()
    module_name = normalize_match_key(getattr(module, "name", ""))
    if module_name and module_name in keyword_terms:
        matched.add(module_name)

    paramlist = getattr(module, "paramlist", None)
    if paramlist is not None:
        for param in getattr(paramlist, "params", []) or []:
            snippet = _safe_codegen(param, codegen)
            matched.update(extract_terms(snippet) & keyword_terms)

    portlist = getattr(module, "portlist", None)
    if portlist is not None:
        for port in getattr(portlist, "ports", []) or []:
            snippet = _safe_codegen(port, codegen)
            matched.update(extract_terms(snippet) & keyword_terms)

    for item in getattr(module, "items", []) or []:
        if not _is_decl_like_item(item):
            continue
        snippet = _safe_codegen(item, codegen)
        matched.update(extract_terms(snippet) & keyword_terms)

    return matched


def _build_ast_module_content(module: Any) -> tuple[str, list[str] | None]:
    try:
        from pyverilog.ast_code_generator.codegen import ASTCodeGenerator
    except Exception as exc:
        return "", [f"codegen_import_error:{exc}"]

    try:
        rebuilt = copy.deepcopy(module)
        rebuilt.items = [item for item in (getattr(rebuilt, "items", []) or []) if _is_decl_like_item(item)]
        codegen = ASTCodeGenerator()
        content = _safe_codegen(rebuilt, codegen).strip()
        return content, None if content else ["empty_codegen_output"]
    except Exception as exc:
        return "", [str(exc)]


def _extract_ast_module_artifacts(text: str, path: Path, keyword_terms: set[str], parser_info: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    if not parser_info.get("success"):
        return [], None

    try:
        from pyverilog.ast_code_generator.codegen import ASTCodeGenerator
        from pyverilog.vparser.parser import parse
    except Exception as exc:
        return [], f"ast_import_error:{exc}"

    try:
        ast, _ = parse([str(path)])
    except Exception as exc:
        return [], f"ast_parse_error:{exc}"

    description = getattr(ast, "description", None)
    definitions = getattr(description, "definitions", []) if description is not None else []
    codegen = ASTCodeGenerator()
    results: list[dict[str, Any]] = []

    for node in definitions:
        if type(node).__name__ != "ModuleDef":
            continue
        matched_terms = sorted(_ast_module_matches_terms(node, keyword_terms, codegen))
        if not matched_terms:
            continue
        content, errors = _build_ast_module_content(node)
        if errors:
            return [], "; ".join(errors)
        if not content:
            continue
        body_line = _first_line(content)
        body_match = re.search(rf"(?P<body>{re.escape(body_line)}[\s\S]*?endmodule)", text, re.IGNORECASE)
        span = _line_span(text, body_match.start("body"), body_match.end("body")) if body_match else {}
        results.append(
            {
                "type": "dut",
                "name": getattr(node, "name", "") or path.stem,
                "content": content,
                "span": span,
                "parser": parser_info,
                "metadata": {
                    "source_type": path.suffix.lower(),
                    "normalized_keywords": sorted(extract_terms(content))[:200],
                    "matched_terms": matched_terms,
                    "selected_definition_names": [getattr(node, "name", "")],
                    "extraction_mode": "ast_module",
                    "parse_success": True,
                    "fallback_reason": None,
                },
            }
        )
    return results, None


def _extract_regex_module_declarations(body: str) -> str:
    cleaned = _remove_cover_statements(body)
    lines = cleaned.splitlines()
    if not lines:
        return ""
    if len(lines) == 1 and "endmodule" in lines[0].lower():
        single_line = re.sub(r"\bendmodule\b", "", lines[0], flags=re.IGNORECASE)
        parts = [part.strip() for part in single_line.split(";") if part.strip()]
        if not parts:
            return body.strip()
        kept_single = [f"{parts[0]};"]
        for part in parts[1:]:
            if DECL_START_RE.match(part):
                kept_single.append(f"  {part};")
        kept_single.append("endmodule")
        return "\n".join(kept_single).strip()

    kept: list[str] = []
    index = 0
    header_done = False
    declaration_buffer: list[str] = []

    while index < len(lines):
        line = lines[index]
        kept.append(line)
        if ";" in line:
            header_done = True
            index += 1
            break
        index += 1

    if not header_done:
        return cleaned.strip()

    while index < len(lines):
        stripped = lines[index].strip()
        lowered = stripped.lower()
        if lowered == "endmodule":
            break
        if not stripped:
            kept.append(lines[index])
            index += 1
            continue
        if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
            kept.append(lines[index])
            index += 1
            continue
        if lowered.startswith("assign ") or lowered.startswith("always") or lowered.startswith("initial"):
            index += 1
            continue
        if DECL_START_RE.match(stripped):
            declaration_buffer = [lines[index]]
            while ";" not in lines[index] and index + 1 < len(lines):
                index += 1
                declaration_buffer.append(lines[index])
            kept.extend(declaration_buffer)
            declaration_buffer = []
        index += 1

    kept.append("endmodule")
    return "\n".join(kept).strip()


def _build_regex_dut_artifact(
    *,
    body: str,
    text: str,
    match: re.Match[str],
    path: Path,
    parser_info: dict[str, Any],
    name: str,
    keyword_terms: set[str],
    extraction_mode: str,
    fallback_reason: str | None,
) -> dict[str, Any] | None:
    cleaned = body.strip() if extraction_mode == "regex_interface" else _extract_regex_module_declarations(body)
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    matched_terms = sorted(extract_terms(cleaned) & keyword_terms)
    if not matched_terms:
        return None
    return {
        "type": "dut",
        "name": name,
        "content": cleaned,
        "span": _line_span(text, match.start("body"), match.end("body")),
        "parser": parser_info,
        "metadata": {
            "source_type": path.suffix.lower(),
            "normalized_keywords": sorted(extract_terms(cleaned))[:200],
            "matched_terms": matched_terms,
            "selected_definition_names": [name],
            "extraction_mode": extraction_mode,
            "parse_success": bool(parser_info.get("success")),
            "fallback_reason": fallback_reason,
        },
    }


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
    if not keyword_terms:
        return []

    text = path.read_text(encoding="utf-8", errors="ignore")
    parser_info = try_parse_with_pyverilog(path)
    results: list[dict[str, Any]] = []
    ast_results, ast_failure_reason = _extract_ast_module_artifacts(text, path, keyword_terms, parser_info)
    results.extend(ast_results)

    if not parser_info.get("success") or ast_failure_reason is not None:
        fallback_reason = ast_failure_reason or parser_info.get("error") or "parser_unavailable"
        for index, match in enumerate(MODULE_RE.finditer(text), start=1):
            name = match.groupdict().get("name") or f"{path.stem}-dut-{index}"
            artifact = _build_regex_dut_artifact(
                body=match.group("body"),
                text=text,
                match=match,
                path=path,
                parser_info=parser_info,
                name=name,
                keyword_terms=keyword_terms,
                extraction_mode="regex_module",
                fallback_reason=fallback_reason,
            )
            if artifact:
                results.append(artifact)

    for index, match in enumerate(INTERFACE_RE.finditer(text), start=1):
        name = match.groupdict().get("name") or f"{path.stem}-dut-{index}"
        artifact = _build_regex_dut_artifact(
            body=match.group("body"),
            text=text,
            match=match,
            path=path,
            parser_info=parser_info,
            name=name,
            keyword_terms=keyword_terms,
            extraction_mode="regex_interface",
            fallback_reason="systemverilog_interface_regex_only",
        )
        if artifact:
            results.append(artifact)
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
