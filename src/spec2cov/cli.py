from __future__ import annotations

from pathlib import Path

import typer

from spec2cov.config import AppConfig, load_config
from spec2cov.logging_utils import setup_logging
from spec2cov.stages import export_jsonl, fetch_filter, gen_retrieve, init_db, preprocess

app = typer.Typer(help="Spec-to-coverage dataset pipeline")


def _load(config_path: Path) -> AppConfig:
    config = load_config(config_path)
    setup_logging(config.log_dir)
    return config


@app.command("init-db")
def init_db_command(config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True)) -> None:
    cfg = _load(config)
    run_id = init_db.run(cfg)
    typer.echo(f"init-db completed (run_id={run_id})")


@app.command("fetch-filter")
def fetch_filter_command(
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True),
    resume: bool = typer.Option(False, help="Only process pending or failed files"),
    max_repos: int | None = typer.Option(None, help="Override max discovered repos"),
    max_files_per_repo: int | None = typer.Option(None, help="Override max files per repo"),
    repo_csv: Path | None = typer.Option(None, exists=True, dir_okay=False, readable=True, help="Optional CSV of GitHub repository links or owner/repo values"),
) -> None:
    cfg = _load(config)
    run_id = fetch_filter.run(cfg, resume=resume, max_repos=max_repos, max_files_per_repo=max_files_per_repo, repo_csv=repo_csv)
    typer.echo(f"fetch-filter completed (run_id={run_id})")


@app.command("preprocess")
def preprocess_command(
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True),
    resume: bool = typer.Option(False, help="Skip repos with existing artifacts"),
) -> None:
    cfg = _load(config)
    run_id = preprocess.run(cfg, resume=resume)
    typer.echo(f"preprocess completed (run_id={run_id})")


@app.command("export-jsonl")
def export_jsonl_command(
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True),
    formats: str = typer.Option("non-agentic,agentic", help="Comma-separated list of formats to export"),
) -> None:
    cfg = _load(config)
    selected = [item.strip() for item in formats.split(",") if item.strip()]
    run_id = export_jsonl.run(cfg, formats=selected)
    typer.echo(f"export-jsonl completed (run_id={run_id})")


@app.command("gen-retrieve")
def gen_retrieve_command(
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True),
) -> None:
    cfg = _load(config)
    run_id = gen_retrieve.run(cfg)
    typer.echo(f"gen-retrieve completed (run_id={run_id})")


@app.command("run-all")
def run_all_command(
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, dir_okay=False, readable=True),
    resume: bool = typer.Option(False, help="Resume processing for fetch and preprocess stages"),
) -> None:
    cfg = _load(config)
    init_db.run(cfg)
    fetch_filter.run(cfg, resume=resume)
    preprocess.run(cfg, resume=resume)
    run_id = gen_retrieve.run(cfg)
    typer.echo(f"run-all stopped after gen-retrieve (run_id={run_id}). Manually review/process preprocess outputs, then run: spec2cov.cli export-jsonl")


if __name__ == "__main__":
    app()
