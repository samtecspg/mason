from typing import Union, Optional, Tuple
from dask.distributed import Client

from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob
import dask.array as da

class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        # dask scheduler location, not to be confused with mason engine scheduler
        self.scheduler = config.get("scheduler")

    def run(self, job: Job, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        final: Union[ExecutedJob, InvalidJob]
        response: Response = resp or Response()
        
        if self.scheduler:
            # Warning: side-effects, client is used by dask implicitly
            client = Client(self.scheduler)
            if isinstance(job, FormatJob):
                array = da.ones((1000, 1000, 1000))
                result =  array.mean().compute()
                # table = job.table
                final = job.errored("Job type not supported for Dask")
            else:
                final = job.errored("Job type not supported for Dask")
        else:
            final =  InvalidJob("Dask Scheduler not defined")
        return final, response


