from __future__ import annotations

import orjson

from spec2cov.config import AppConfig


def build_record(sample: dict, config: AppConfig) -> dict:
    index = int(sample["sample_id"])
    sample_key = sample["sample_key"]
    categories = orjson.loads(sample["categories_json"])
    input_context = orjson.loads(sample["input_artifacts_json"])
    output_context = orjson.loads(sample["output_artifacts_json"])
    patch_context = {key: value for key, value in output_context.items() if key.endswith((".sv", ".hvp", ".txt"))}
    return {
        "id": f"{config.export.dataset_prefix}_agent_{sample_key}_{index:04d}",
        "categories": categories,
        "system_message": config.export.system_message,
        "prompt": config.export.prompt_template,
        "context": input_context,
        "patch": patch_context,
        "harness": {},
    }
