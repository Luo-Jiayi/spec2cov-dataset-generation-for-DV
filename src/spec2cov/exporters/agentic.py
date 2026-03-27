from __future__ import annotations

import orjson

from spec2cov.config import AppConfig


def build_record(
    sample: dict,
    config: AppConfig,
    prompt_template: str | None = None,
    system_message: str | None = None,
) -> dict:
    index = int(sample["sample_id"])
    sample_key = sample["sample_key"]
    categories = orjson.loads(sample["categories_json"])
    input_context = orjson.loads(sample["input_artifacts_json"])
    output_context = orjson.loads(sample["output_artifacts_json"])
    patch_context = {key: value for key, value in output_context.items() if key.endswith((".sv", ".hvp", ".txt"))}
    prompt = prompt_template if prompt_template is not None else config.export.prompt_template
    system = system_message if system_message is not None else config.export.system_message
    return {
        "id": f"{config.export.dataset_prefix}_agent_{sample_key}_{index:04d}",
        "categories": categories,
        "system_message": system,
        "prompt": prompt,
        "context": input_context,
        "patch": patch_context,
        "harness": {},
    }
