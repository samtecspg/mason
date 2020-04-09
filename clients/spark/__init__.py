
from clients.response import Response
from clients.spark.runner.kubernetes_operator import KubernetesOperator
from clients.spark.config import SparkConfig
from clients.spark.runner import EmptySparkRunner
from engines.metastore.models.credentials import MetastoreCredentials

class SparkClient:
    def __init__(self, spark_config: dict):
        self.runner_type = spark_config.get("runner", {}).get("type", "")
        self.config = SparkConfig(spark_config)

    def run_job(self, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        runner = self.get_runner(self.runner_type)
        runner.run(self.config, job_name, metastore_credentials, params, response)
        response.add_info(f"Running job {job_name}")
        return response

    def get_job(self, job_id: str, response: Response):
        runner = self.get_runner(self.runner_type)
        runner.get(job_id, response)
        return response




    def get_runner(self, runner: str):
        if runner == "kubernetes-operator":
            return KubernetesOperator()
        else:
            return EmptySparkRunner()
