from __future__ import annotations

import orjson

from spec2cov.config import AppConfig


def build_record(
    sample: dict,
    config: AppConfig,
    prompt_template: str | dict[str, str] | None = None,
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

    if isinstance(prompt, dict):
        prompt_feature = str(prompt.get("user_prompt_feature", ""))
        prompt_fp = str(prompt.get("user_prompt_fp", ""))
        prompt_cover = str(prompt.get("user_prompt_cover", ""))
    else:
        prompt_feature = str(prompt)
        prompt_fp = ""
        prompt_cover = ""

    return {
        "id": f"{config.export.dataset_prefix}_agent_{sample_key}_{index:04d}",
        "categories": categories,
        "system_message": system,
        # structured prompts mapped to nodes / stages
        "prompts": {
            "feature": {
                "type": "user_prompt_feature",
                "content": prompt_feature,
            },
            "function_point": {
                "type": "user_prompt_fp",
                "content": prompt_fp,
            },
            "coverage": {
                "type": "user_prompt_cover",
                "content": prompt_cover,
            },
        },
        # optional execution order (important for agent orchestration)
        "prompt_sequence": ["feature", "function_point", "coverage"],
        "context": input_context,
        "patch": patch_context,
        "harness": {},
    }
