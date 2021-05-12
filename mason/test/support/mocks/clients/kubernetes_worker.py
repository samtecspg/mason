from typing import Tuple, Union, Optional

from mason.clients.dask.runner.kubernetes_worker.kubernetes_worker import KubernetesWorker
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.engines.execution.models.jobs.query_job import QueryJob

class KubernetesWorkerMock():

    def run(self, job: Job, resp: Optional[Response] = None, mode: str = "sync") -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        # TODO:  Figure out how to get init variables through MagicMock so i can test scheduler with this test case:
        # InvalidJob Timed out trying to connect # OSError: Timed out trying to connect to 'tcp://dask-scheduler:8786'
        r = resp or Response()
        if isinstance(job, FormatJob):
            if job.format == "csv" and job.output_path.path_str == "s3://good_output_path":
                return (ExecutedJob('Table successfully formatted as csv'), r)
            else:
                return (InvalidJob('Invalid Dask Job: Invalid Schema'), r)
        elif isinstance(job, QueryJob):
            job.output_path.protocol  = "file"
            return (KubernetesWorker({"scheduler": "local:8786"}).run_job(job, "local:8786", mode), r)
        else:
            raise Exception(f"Mock job not implemented: {job.type}")

