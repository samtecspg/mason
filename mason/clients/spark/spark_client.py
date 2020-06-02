from typing import Union

from mason.clients.response import Response
from mason.clients.spark.config import SparkConfig
from mason.clients.spark.runner.spark_runner import EmptySparkRunner
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import KubernetesOperator
from mason.engines.execution.models.jobs import ExecutedJob, Job, InvalidJob


class SparkClient:
    def __init__(self, spark_config: dict):
        self.runner_type = spark_config.get("runner", {}).get("type", "")
        self.config = SparkConfig(spark_config)

    def client(self):
        return self.get_runner(self.runner_type)

    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return self.client().run(self.config, job)

    def get_job(self, job_id: str, response: Response) -> ExecutedJob:
        job = self.client().get(job_id, response)

        return job

    def get_runner(self, runner: str):
        if runner == "kubernetes-operator":
            return KubernetesOperator()
        else:
            return EmptySparkRunner()
