from typing import Tuple, Union

from mason.clients.engines.execution import ExecutionClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs.summary_job import SummaryJob
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path

class DeferredTableSummary(TableSummary):
    def __init__(self, table_name: str, path: Path, credentials: Union[MetastoreCredentials, InvalidCredentials]):
        self.path = path
        self.job = SummaryJob(path, credentials)
        super().__init__()
    
    def run(self, execution: ExecutionClient, response: Response = Response()) -> Tuple['DeferredTableSummary', Response]:
        run, response = execution.run_job(self.job, response)
        self.job_run = run
        response = run.to_response(response)

        return self, response
