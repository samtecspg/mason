from engines.execution.models.jobs import Job

class DaskConfig:
    def __init__(self, config: dict):
        self.scheduler = config.get("scheduler")

    def job_name(self, job: Job):
        return "dask_" + job.type + "_" + job.id


