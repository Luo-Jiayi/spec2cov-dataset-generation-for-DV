from __future__ import annotations

import orjson

from spec2cov.config import AppConfig


def build_record(sample: dict, config: AppConfig, prompt_template: str | None = None) -> dict:
    index = int(sample["sample_id"])
    sample_key = sample["sample_key"]
    categories = orjson.loads(sample["categories_json"])
    input_context = orjson.loads(sample["input_artifacts_json"])
    output_context = orjson.loads(sample["output_artifacts_json"])
    prompt = prompt_template if prompt_template is not None else config.export.prompt_template
    return {
        "id": f"{config.export.dataset_prefix}_copilot_{sample_key}_{index:04d}",
        "categories": categories,
        "input": {
            "prompt": prompt,
            "context": input_context,
        },
        "output": {
            "response": "",
            "context": output_context,
        },
        "harness": {},
    }
