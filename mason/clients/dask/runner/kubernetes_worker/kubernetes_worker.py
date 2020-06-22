from typing import Union, Optional, Tuple
from dask.distributed import Client

from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job


class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        self.scheduler = config.get("scheduler")

    #  No valid Dask implementations until v 1.05
    def run(self, job: Job, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        final: Union[ExecutedJob, InvalidJob]
        response: Response = resp or Response()
        
        if self.scheduler:
            # Warning: side-effects, client is used by dask implicitly
            client = Client(self.scheduler, asynchronous=True)
            final = job.errored("Job type not supported for Dask")
        else:
            final =  InvalidJob("Dask Scheduler not defined")
        return final, response


