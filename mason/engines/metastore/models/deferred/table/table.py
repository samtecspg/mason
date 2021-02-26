from typing import Union, Tuple

from mason.clients.engines.execution import ExecutionClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs.infer_job import InferJob
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.schemas.schema import EmptySchema
from mason.engines.metastore.models.table import Table
from mason.engines.storage.models.path import Path

class DeferredTable(Table):
    
    def __init__(self, table_name: str, path: Path, credentials: Union[MetastoreCredentials, InvalidCredentials]):
        self.path = path
        self.job = InferJob(path, credentials)
        super().__init__(table_name, EmptySchema())
        
    def run(self, execution: ExecutionClient, response: Response = Response()) -> Tuple['DeferredTable', Response]:
        run, response = execution.run_job(self.job, response)
        self.job_run = run
        response = run.to_response(response)
        
        return self, response

    def to_dict(self):
        return {}

