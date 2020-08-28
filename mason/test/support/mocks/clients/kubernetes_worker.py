from typing import Tuple, Union, Optional

from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob


class KubernetesWorkerMock():

    def run(self, job: Job, resp: Optional[Response] = None, mode: str = "async") -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        # TODO:  Figure out how to get init variables through MagicMock so i can test scheduler with this test case:
        # InvalidJob Timed out trying to connect # OSError: Timed out trying to connect to 'tcp://dask-scheduler:8786'
        r = resp or Response()
        if isinstance(job, FormatJob):
            if job.format == "csv" and job.output_path.path_str == "good_output_path":
                return (ExecutedJob('Table succesfully formatted as csv'), r)
            else:
                return (InvalidJob('Invalid Dask Job: Invalid Schema'), r)

