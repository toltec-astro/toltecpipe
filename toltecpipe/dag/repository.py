from dagster import repository
import os

from .jobs.toltec_timely_analysis import make_toltec_timely_analysis_jobs
from .resources import resource_defs_by_deployment_name


@repository
def toltecpipe():
    deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
    resource_defs = resource_defs_by_deployment_name[deployment_name]
    repo = [
        *make_toltec_timely_analysis_jobs(resource_defs),
    ]
    return repo
