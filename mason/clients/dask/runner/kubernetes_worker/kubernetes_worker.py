from typing import Union, Optional, Tuple
from dask.distributed import Client

from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob
from dask.dataframe.core import DataFrame
import dask.dataframe as dd

from mason.engines.metastore.models.schemas.text import TextSchema


class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        # dask scheduler location, not to be confused with mason engine scheduler
        self.scheduler = config.get("scheduler")

    def run(self, job: Job, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        final: Union[ExecutedJob, InvalidJob]
        response: Response = resp or Response()
        
        supported_output_formats = [ "csv" ]

        if self.scheduler:
            # Warning: side-effects, client is used by dask implicitly
            client = Client()
            
            if isinstance(job, FormatJob):
                format = job.format
                if format in supported_output_formats:
                    if format == "csv":
                        input_format = job.table.schema.type
                        
                        if isinstance(job.table.schema, TextSchema):
                            paths = ",".join(list(map(lambda p: p.full_path(),job.table.paths)))
                            df: DataFrame = dd.read_csv(paths, lineterminator=job.table.schema.line_terminator)
                            df.to_csv(job.output_path.path_str)
                            final = ExecutedJob("format_job", f"Table of format {input_format} formatted as {format} and exported to {job.output_path.path_str}")
                        else:
                            final = InvalidJob(f"Input Format {input_format} not supported for format implementation.")
                    else:
                        final = InvalidJob(f"Output Format {format} not supported")
                else:
                    final = InvalidJob(f"Output Format {format} not supported")
            else:
                final = job.errored("Job type not supported for Dask")
        else:
            final = InvalidJob("Dask Scheduler not defined")
            
        return final, response


