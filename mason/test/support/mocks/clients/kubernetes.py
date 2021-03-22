from typing import Optional, Union, Tuple

from mason.clients.response import Response
from mason.clients.spark.spark_client import SparkConfig

from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.execution.models.jobs.merge_job import MergeJob

class KubernetesMock:

    def run(self, config: SparkConfig, job: Job, resp: Response = Response()) -> Optional[Job]:
        params = job.parameters
        if isinstance(job, MergeJob) and isinstance(params, dict):
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
        else:
            raise Exception("Job type not supported")

    def get(self, job_id: str, response: Response) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        job = Job("spark")
        job.set_id(job_id)
        if job_id == "good_job_id":
            logs = '<LOG_DATA>'
            response.add_data({'Logs': [logs]})
            return job.running(past=True), response
        elif job_id == "bad_job_id":
            error = "Error from server (NotFound): pods \"bad_job_id-driver\" not found"
            response.add_error(error)
            response.set_status(400)
            return job.errored(), response
        else:
            return InvalidJob("Mock parameters not implemented for spark kubernetes implementation"), response





