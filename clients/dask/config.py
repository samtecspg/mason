from engines.execution.models.jobs import Job

class DaskConfig:
    def __init__(self, config: dict):

        self.app_name = "mason-dask"
        self.threads = config.get("threads", 2)
        self.worker_memory_gb = config.get("memory_mb", 6)
        self.timeout = config.get("timeout", 60)
        self.cpus = config.get("cpus", 2)
        self.docker_image = config.get("docker_image", "daskdev/dask:latest")
        self.memory_gb = config.get("memory_mb", 6)

    def job_name(self, job: Job):
        return self.app_name + "-" + job.type + "-" + job.id


