from typing import Optional, Tuple, Union

from mason.util.logger import logger
# from xlsxwriter import Workbook
# import fsspec
# from mason.engines.execution.models.jobs.format_job import FormatJob

from mason.clients.local.local_client import LocalClient

from mason.clients.response import Response

from mason.clients.engines.execution import ExecutionClient
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class LocalExecutionClient(ExecutionClient):
    
    def __init__(self, config: dict):
        self.client = LocalClient(config)

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:

        resp: Response = response or Response()
        # if isinstance(job, FormatJob):
            # for path in job.paths:
            #     workbook = Workbook(path.path_str + '.xlsx')
            #     worksheet = workbook.add_worksheet()
            #     with fsspec.open(path.full_path(), mode='rt') as f:
            #         f.read()
                #     for col in split:
                #         worksheet.writer(row,col, i)
                #         i+=1
                #             
                # workbook.close()
        # else:
        final = InvalidJob(f"Job type {job.type} not supported")
            
        return final, resp

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

