from typing import Union, Tuple

from dask.distributed import Client
import dask.array as da

from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob

class DaskFormatJob:
    
    def run(self, client: Client, response: Response) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        client
        array = da.ones((1000, 1000, 1000))
        result = array.mean().compute()
        job = ExecutedJob(f"RESULT: {result}")
        return job, response
