# spec2cov

A Python pipeline for building a spec-to-coverage dataset and benchmark database from open-source GitHub SystemVerilog / UVM repositories.
## workflow
use workflow from OpenLLM-RTL: Open Dataset and Benchmark for LLM-Aided Design RTL Generation: Invited Paper
1. use Google BigQuery or GitHub REST api to gather Verilog repositories from GitHub. use a query that looks for keywords such as "Verilog" "SystemVerilog" "uvm" and files with extensions '.v' '.sv' (source files) or '.md' (specification files) or '.xml' '.xlsx' (testplan files) or '.ralf' (ralgen files) or '.hvp' (hierarchical verification plan files). de-duplicated files (using MinHash and Jaccard similarity metrics) and filter files by keeping '.v' '.sv' files that contain keywords 'covergroup' 'bins' 'coverpoint' 'cover'.
2. also import csv files (例如ref:https://github.com/mayurkubavat/UVM-Examples 这个仓库给出了多个UVM项目，https://blog.csdn.net/zhajio/article/details/110846081 这个网页上给出了一些github仓库的链接，可以省了检索直接把链接到db里)
4. use Pyverilog to extract to abstract syntax tree from Verilog/SystemVerilog code and employ the following filtering process to identify coverage model, interface and module definition blocks from open-sourced Github Verilog code
5. output jsonl in CVDP compatible format

## Current status

The repository now contains a working first implementation of the pipeline skeleton, including:

- SQLite database schema for repositories, files, fetch attempts, filters, dedup signatures, extracted artifacts, quality gates, samples, exports, and pipeline runs
- Stage-based CLI:
  - `init-db`
  - `fetch-filter`
  - `preprocess`
  - `export-jsonl`
  - `run-all`
- Config-driven paths, thresholds, and runtime settings
- GitHub file-level fetching without cloning whole repositories
- GitHub REST API discovery module for candidate repositories/files, including optional CSV repo import
- Verilog/SystemVerilog keyword filtering
- MinHash + Jaccard based dedup support
- Preprocessing for:
  - `.xml` / `.xlsx` -> `plan` (saved as Markdown tables in `*-plan.txt`)
  - `.hvp` -> `hvp`
  - `.ralf` -> `cover`
  - `.pdf` -> `spec` (converted to Markdown-style text and filtered by cover-driven keywords)
  - `.v` / `.sv` -> `cover` first, then keyword-filtered `dut`
  - `.md` -> keyword-windowed `spec`
  - `.md` mentioning `.pdf` -> empty `spec` placeholder file with spec-short exemption
- Preprocess now follows a cover-driven order: `plan/hvp -> cover -> spec/dut`
- Artifacts are grouped into repo-internal projects and saved as `proj[index]-[name]-{plan,hvp,cover,spec,dut}.txt`, where `index` is a project cluster id rather than a simple write order
- JSONL exporters for both non-agentic and agentic benchmark formats
- Basic tests covering filter logic, dedup, quality gates, and exporter shapes

## Project layout

- `config/default.yaml` - pipeline configuration
- `src/spec2cov/cli.py` - CLI entrypoint
- `src/spec2cov/db/` - schema and database access
- `src/spec2cov/sources/` - BigQuery discovery and GitHub fetch
- `src/spec2cov/filtering/` - keyword filtering and dedup
- `src/spec2cov/parsing/` - document and SV extraction
- `src/spec2cov/quality/` - repository quality gates
- `src/spec2cov/stages/` - pipeline stages
- `src/spec2cov/exporters/` - JSONL output builders
- `tests/` - unit tests

## Setup

Create and use the local virtual environment:

```bash
python -m venv .venv
./.venv/Scripts/python -m pip install -e ".[dev]"
```

## Commands

Initialize database and directories:

```bash
./.venv/Scripts/python -m spec2cov.cli init-db
```

(resume选项跳过已有，如果要忽略db里的值重新构建就不要加)
Fetch, persist discovered repositories, enrich them via GitHub API, and then filter/deduplicate candidate files:

```bash
./.venv/Scripts/python -m spec2cov.cli fetch-filter --resume
```

Import additional repository links from a CSV while running fetch-filter:

```bash
./.venv/Scripts/python -m spec2cov.cli fetch-filter --repo-csv repos.csv --resume
```

Preprocess raw files into extracted artifacts:

```bash
./.venv/Scripts/python -m spec2cov.cli preprocess --resume
```

Export benchmark JSONL files:

```bash
./.venv/Scripts/python -m spec2cov.cli export-jsonl
```

Run the whole pipeline:

```bash
./.venv/Scripts/python -m spec2cov.cli run-all --resume
```

Run tests:

```bash
./.venv/Scripts/python -m pytest
```

## Verified so far

- Virtual environment created successfully
- Project dependencies installed successfully
- Test suite passing (`37 passed`)
- CLI help works
- `init-db` stage runs successfully and creates `data/pipeline.db`
- `fetch-filter` stage works with github api and pulls files onto `data/raw/`, updates db
- `preprocess` stage can output files in `data/preprocess/`, though with unsatisfying quality （dut去除always assign block的逻辑有误，还是按照regex而不是ast，应当改成正向用cover的关键词检索需要的module & signal def；cluster也不准确，丢失很多文件；很多仓库缺少文档，考虑后续ai生成补上）

## GitHub token

Set a GitHub REST API token before running discovery and fetching.

You can either put it in the shell environment:

```bash
set GITHUB_TOKEN=your_token_here
```

PowerShell:

```powershell
$env:GITHUB_TOKEN="your_token_here"
```

Or place it in the project root `.env` file:

```env
GITHUB_TOKEN=your_token_here
```

The pipeline now loads `.env` automatically from the project root when reading [config/default.yaml](config/default.yaml). It uses the token name from `runtime.github_token_env`, defaults to `GITHUB_TOKEN`, applies a configurable request interval, and checks `x-ratelimit-*` headers to detect low quota and primary rate-limit exhaustion.

## Notes

- Discovery now uses the GitHub REST API rather than BigQuery, so end-to-end execution depends on a valid GitHub token, network access, and GitHub rate limits.
- Repositories found by search are now inserted into the database before metadata enrichment and file discovery, and `fetch-filter` can merge an optional CSV repo list with searched repos.
- Rate limiting is now controlled by `discovery.request_interval_sec`, `discovery.low_remaining_threshold`, `discovery.rate_limit_buffer_sec`, `discovery.secondary_limit_wait_sec`, and `discovery.secondary_limit_max_retries` in [config/default.yaml](config/default.yaml).
- Requests are issued serially, search pagination follows the `Link` header, and file content is fetched from GitHub blob API URLs returned by the tree API.
- When GitHub returns `retry-after`, the fetcher waits that duration before retrying. When GitHub returns `403` or `429` with `x-ratelimit-remaining: 0`, the fetcher waits until `x-ratelimit-reset` and retries only after that reset point.
- `Pyverilog` is used as the primary parser hook for `.v` / `.sv`, with pragmatic regex/text extraction retained for coverage and DUT blocks because real-world SystemVerilog/UVM syntax support is incomplete.
- `.pdf` files are now fetched and saved in binary-safe mode during `fetch-filter`, and `preprocess` consumes the preserved PDF bytes with `pypdf`.
- DUT extraction now excludes `always` / `initial` / `assign` and skips cover-related content.
- Spec extraction is now driven by cover-derived keywords and keeps only matched keyword windows rather than broad document slices.
- The current implementation is a solid runnable foundation intended for iterative refinement of query quality, parsing robustness, multi-project clustering quality, and sample construction quality.
