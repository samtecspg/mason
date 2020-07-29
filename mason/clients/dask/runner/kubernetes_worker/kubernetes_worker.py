from typing import Union, Optional, Tuple
from dask.distributed import Client

from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.dask.runner.kubernetes_worker.jobs.format import DaskFormatJob
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.util.logger import logger


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
                final, response = DaskFormatJob().run(client, response)
            else:
                final = job.errored("Job type not supported for Dask")
        else:
            final =  InvalidJob("Dask Scheduler not defined")
        logger.remove("HERE")
        return final, response


