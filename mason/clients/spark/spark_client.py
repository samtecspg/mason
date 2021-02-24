from typing import Union, Tuple, Optional

from mason.clients.base import Client
from mason.clients.response import Response
from mason.clients.spark.config import SparkConfig
from mason.clients.spark.runner.spark_runner import EmptySparkRunner
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import KubernetesOperator
from mason.engines.execution.models.jobs import ExecutedJob, Job, InvalidJob

class SparkClient(Client):
    def __init__(self, runner: dict):
        self.runner_type = runner["type"]
        self.config = SparkConfig(runner)

    def to_dict(self) -> dict:
        return {
            'client_name': super().name()
        }

    def client(self):
        return self.get_runner(self.runner_type)

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return self.client().run(self.config, job, response)

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        job = self.client().get(job_id, response)

        return job

    def get_runner(self, runner: str):
        if runner == "kubernetes-operator":
            return KubernetesOperator()
        else:
            return EmptySparkRunner()
