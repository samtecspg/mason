
from clients.response import Response
from clients.spark.runner.kubernetes_operator import KubernetesOperator
from clients.spark.config import SparkConfig
from clients.spark.runner import EmptySparkRunner
from engines.metastore.models.credentials import MetastoreCredentials
from clients.engines.execution import ExecutionClient

class SparkClient(ExecutionClient):
    def __init__(self, spark_config: dict):
        self.runner_type = spark_config.get("runner", {}).get("type", "")
        self.config = SparkConfig(spark_config)

    def client(self):
        return self.get_runner(self.runner_type)

    def run_job(self, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        self.client().run(self.config, job_name, metastore_credentials, params, response)
        response.add_info(f"Running job {job_name}")
        return response

    def get_job(self, job_id: str, response: Response):
        self.client().get(job_id, response)
        return response

    def get_runner(self, runner: str):
        if runner == "kubernetes-operator":
            return KubernetesOperator()
        else:
            return EmptySparkRunner()
