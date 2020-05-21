import hiyapyco

# from definitions import from_root
from engines.execution.models.jobs import Job
# from hiyapyco import load as hload
# import yaml

class DaskConfig:
    def __init__(self, config: dict):
        # self.app_name = "mason-dask"
        # self.threads = config.get("threads", 2)
        # self.worker_memory_gb = config.get("memory_mb", 6)
        # self.timeout = config.get("timeout", 60)
        # self.cpus = config.get("cpus", 2)
        # self.docker_image = config.get("docker_image", "daskdev/dask:latest")
        # self.memory_gb = config.get("memory_mb", 6)
        # self.workers = config.get("workers", 2)
        self.scheduler = config.get("scheduler")

    # def to_dict(self, job: Job):
        # base_config_file = from_root("/clients/dask/runner/kubernetes_worker/base_config.yaml")
        #
        # job_name = job.type
        # merge_document = {
        #     'metadata': {
        #         'name': job_name
        #     }
        # }
        #
        # arguments = yaml.dump(merge_document)
        # conf = hload(base_config_file, arguments, method=hiyapyco.METHOD_MERGE, usedefaultyamlloader=True)
        # logger.remove(conf)
        # return conf

    def job_name(self, job: Job):
        return self.app_name + "-" + job.type + "-" + job.id


