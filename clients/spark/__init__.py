
from clients.response import Response

class SparkClient:
    def __init__(self, spark_config: dict):
        self.threads = spark_config.get("threads")
        # self.client = ###  Depends on job scheduler, k8s or EMR, etc

    def run_job(self, job_name: str, response: Response):
        response.add_info(f"Running job {job_name}")


