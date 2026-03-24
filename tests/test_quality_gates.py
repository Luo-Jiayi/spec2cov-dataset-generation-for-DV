from spec2cov.config import QualityGateConfig
from spec2cov.quality.gates import evaluate_repo_quality


def test_quality_gate_passes_with_minimum_artifacts():
    artifacts = [
        {"artifact_type": "cover", "char_count": 50},
        {"artifact_type": "plan", "char_count": 50},
        {"artifact_type": "dut", "char_count": 50},
        {"artifact_type": "spec", "char_count": 150},
    ]
    passed, score, counts, reason = evaluate_repo_quality(artifacts, QualityGateConfig())
    assert passed is True
    assert score > 0
    assert counts["cover"] == 1
    assert reason is None


def test_quality_gate_fails_without_cover():
    artifacts = [
        {"artifact_type": "plan", "char_count": 50},
        {"artifact_type": "dut", "char_count": 50},
        {"artifact_type": "spec", "char_count": 150},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig())
    assert passed is False
    assert "insufficient_cover_blocks" in reason


def test_quality_gate_fails_when_plan_is_low_and_spec_is_short():
    artifacts = [
        {"artifact_type": "cover", "char_count": 50},
        {"artifact_type": "dut", "char_count": 50},
        {"artifact_type": "spec", "char_count": 60},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig())
    assert passed is False
    assert "insufficient_plan_and_spec" in reason


def test_quality_gate_passes_when_plan_is_low_but_spec_is_long_enough():
    artifacts = [
        {"artifact_type": "cover", "char_count": 50},
        {"artifact_type": "dut", "char_count": 50},
        {"artifact_type": "spec", "char_count": 150},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig())
    assert passed is True
    assert reason is None


def test_quality_gate_skips_spec_short_when_flag_enabled():
    artifacts = [
        {"artifact_type": "cover", "char_count": 50},
        {"artifact_type": "dut", "char_count": 50},
        {"artifact_type": "spec", "char_count": 0},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig(), skip_spec_short_check=True)
    assert passed is True
    assert reason is None


def test_quality_gate_skip_flag_does_not_bypass_cover_or_dut_failures():
    artifacts = [
        {"artifact_type": "spec", "char_count": 0},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig(), skip_spec_short_check=True)
    assert passed is False
    assert "insufficient_cover_blocks" in reason
    assert "insufficient_dut_blocks" in reason


def test_quality_gate_fails_without_dut():
    artifacts = [
        {"artifact_type": "cover", "char_count": 50},
        {"artifact_type": "plan", "char_count": 50},
        {"artifact_type": "spec", "char_count": 150},
    ]
    passed, _, _, reason = evaluate_repo_quality(artifacts, QualityGateConfig())
    assert passed is False
    assert "insufficient_dut_blocks" in reason
