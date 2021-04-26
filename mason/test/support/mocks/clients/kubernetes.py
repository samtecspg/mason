from typing import Optional, Union, Tuple

from mason.clients.response import Response
from mason.clients.spark.config import SparkConfig
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import KubernetesOperator
from mason.engines.execution.models.jobs import ExecutedJob, Job, InvalidJob


class KubernetesMock(KubernetesOperator):

    def run(self, config: SparkConfig, job: Job, resp: Response = Response()) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        params = job.parameters
        if job.type in ["summary", "merge"]:
            log = 'sparkapplication.sparkoperator.k8s.io/mason-spark-merge created'
            final = ExecutedJob(job.id, logs=[log])
            return final, resp
        else:
            raise Exception("Job type not supported")

    def get(self, job_id: str, response: Response = Response()) -> Tuple[ExecutedJob, Response]:
        if job_id == "good_job_id":
            logs = ['<LOG_DATA>']
            job = ExecutedJob(job_id, logs=logs)
            return job, response
        elif job_id == "bad_job_id":
            logs = ["Error from server (NotFound): pods \"bad_job_id-driver\" not found"]
            response.set_status(400)
            job = ExecutedJob(job_id, logs=logs)
            return job, response
        else:
            return ExecutedJob(job_id, message="Mock parameters not implemented for spark kubernetes implementation"), response

