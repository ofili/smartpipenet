from __future__ import annotations

from datetime import datetime

from apache_beam.options.pipeline_options import PipelineOptions

from config.config import NUM_WORKERS


def pipeline_options(
        project: str,
        job_name: str,
        mode: str,
        num_workers: int = NUM_WORKERS,
        streaming: bool = True,
):
    job_name = f"{job_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    print(job_name)

    # For a list of available options, check:
    # https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options
    dataflow_options: dict[str, str | bool | int] = {
        "runner": "DirectRunner" if mode == "local" else "DataflowRunner",
        "job_name": job_name,
        "project": project,
        "region": "us-central1",
        "setup_file": "./setup.py",
        "streaming": streaming,
    }

    # Optional parameters
    if num_workers:
        dataflow_options.update({"num_workers": num_workers})

    return PipelineOptions(flags=[], **dataflow_options)
