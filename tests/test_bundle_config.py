"""Checks on the Databricks Asset Bundle configuration."""
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ["dev", "stage", "prod"]


def load_yaml(relative_path):
    with (ROOT / relative_path).open(encoding="utf-8") as file:
        return yaml.safe_load(file)


BUNDLE = load_yaml("databricks.yml")
PIPELINE = load_yaml("resources/veloradar.pipeline.yml")["resources"]["pipelines"]["veloradar_pipeline"]
JOB = load_yaml("resources/veloradar.job.yml")["resources"]["jobs"]["veloradar_job"]


def test_bundle_has_dev_stage_and_prod():
    assert set(BUNDLE["targets"]) == set(TARGETS)


@pytest.mark.parametrize("target", TARGETS)
def test_target_uses_its_own_catalog_and_volume(target):
    variables = BUNDLE["targets"][target]["variables"]
    assert variables["catalog"] == f"veloradar_{target}"
    assert variables["source_path"] == f"/Volumes/veloradar_{target}/landing/events/"


@pytest.mark.parametrize("target", TARGETS)
def test_target_mode(target):
    expected = "development" if target == "dev" else "production"
    assert BUNDLE["targets"][target]["mode"] == expected


def test_pipeline_is_serverless_and_parameterized():
    assert PIPELINE["serverless"] is True
    assert PIPELINE["catalog"] == "${var.catalog}"
    assert PIPELINE["configuration"]["source_path"] == "${var.source_path}"


def test_job_is_paused_and_runs_the_pipeline():
    schedule = JOB["schedule"]
    assert schedule["quartz_cron_expression"] == "0 0/15 * * * ?"
    assert schedule["pause_status"] == "PAUSED"
    task = JOB["tasks"][0]
    assert task["pipeline_task"]["pipeline_id"] == "${resources.pipelines.veloradar_pipeline.id}"
