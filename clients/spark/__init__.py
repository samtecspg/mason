from clients.response import Response
from clients.spark.runner.kubernetes_operator import KubernetesOperator
from clients.spark.config import SparkConfig
from clients.spark.runner import EmptySparkRunner
from engines.metastore.models.credentials import MetastoreCredentials

class SparkClient:
    def __init__(self, spark_config: dict):
        self.runner_type = spark_config.get("runner", {}).get("type", "")
        self.config = SparkConfig(spark_config)

    def client(self):
        return self.get_runner(self.runner_type)

    def run_job(self, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        job = self.client().run(self.config, job_name, metastore_credentials, params)
        response = job.running(response)
        return response

    def get_job(self, job_id: str, response: Response):
        job = self.client().get(job_id)
        response = job.add_data(response)
        not_found = False

        for e in job.errors or []:
            if "not found" in e:
                not_found = True
        if not_found:
            response.set_status(400)

        return response

    def get_runner(self, runner: str):
        if runner == "kubernetes-operator":
            return KubernetesOperator()
        else:
            return EmptySparkRunner()
