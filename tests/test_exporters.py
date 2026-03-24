import orjson

from spec2cov.config import load_config
from spec2cov.exporters.agentic import build_record as build_agentic
from spec2cov.exporters.non_agentic import build_record as build_non_agentic


def sample(config_path="d:/myFiles/main/rag/my_dataset/config/default.yaml"):
    cfg = load_config(config_path)
    return cfg, {
        "sample_id": 1,
        "sample_key": "demo_repo",
        "categories_json": orjson.dumps([cfg.export.default_category, cfg.export.default_difficulty]).decode(),
        "input_artifacts_json": orjson.dumps({"docs/spec.md": "spec"}).decode(),
        "output_artifacts_json": orjson.dumps({"verif/tb.sv": "covergroup cg; endgroup"}).decode(),
    }


def test_non_agentic_record_shape():
    cfg, row = sample()
    record = build_non_agentic(row, cfg)
    assert record["id"].startswith("cvdp_copilot_")
    assert "input" in record and "output" in record


def test_agentic_record_shape():
    cfg, row = sample()
    record = build_agentic(row, cfg)
    assert record["id"].startswith("cvdp_agent_")
    assert "patch" in record
