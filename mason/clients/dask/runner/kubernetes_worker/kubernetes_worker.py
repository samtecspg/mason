from typing import Optional, Tuple

from mason.engines.execution.models.jobs.query_job import QueryJob

from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.dask.runner.kubernetes_worker.jobs.base import run_job
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob

from typing import Union

from mason.util.exception import message


class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        # dask scheduler location, not to be confused with mason engine scheduler
        self.scheduler = config.get("scheduler")

    def run(self, job: Job, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        final: Union[ExecutedJob, InvalidJob]
        response: Response = resp or Response()

        try:
            if self.scheduler:
                if isinstance(job, FormatJob):
                    run_job(job.type, job.spec(), self.scheduler)
                    final = ExecutedJob("format_job", f"Job queued to format {job.table.schema.type} table as {job.format} and save to {job.output_path.path_str}")
                elif isinstance(job, QueryJob):
                    run_job(job.type, job.spec(), self.scheduler)
                else:
                    final = job.errored("Job type not supported for Dask")
            else:
                final = InvalidJob("Dask Scheduler not defined")
        except OSError as e:
            final = InvalidJob(message(e))

        return final, response


