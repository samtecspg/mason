from typing import Optional, Union

from mason.clients.response import Response
from mason.clients.spark.spark_client import SparkConfig

from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import MetastoreCredentials

class KubernetesMock:

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict) -> Optional[Job]:
        if params["input_path"] == "s3a://good_input_bucket/good_input_path" and params["output_path"] == "s3a://good_output_bucket/good_output_path":
            log = 'sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created'
            job = Job("merge")
            job.add_log(log)
            return job
        elif params["input_path"] == "s3a://good_input_bucket_2/good_input_path" and params["output_path"] == "s3a://good_output_bucket/good_output_path":
            log = 'sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created'
            job = Job("merge")
            job.add_log(log)
            return job
        else:
            raise Exception("Mock parameters not implemented for spark kubernetes implementation")

    def get(self, job_id: str, response: Response) -> Union[ExecutedJob, InvalidJob]:
        job = Job("spark", response=response)
        job.set_id(job_id)
        if job_id == "good_job_id":
            logs = '<LOG_DATA>'
            job.response.add_data({'Logs': [logs]})
            return ExecutedJob(job)
        elif job_id == "bad_job_id":
            error = "Error from server (NotFound): pods \"bad_job_id-driver\" not found"
            job.response.add_error(error)
            job.response.set_status(400)
            return ExecutedJob(job)
        else:
            return InvalidJob(job, "Mock parameters not implemented for spark kubernetes implementation")





