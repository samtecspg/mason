from typing import Union, Tuple

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.storage.models.path import Path

class InferJob(Job):

    def __init__(self, database_name: str, table_name: str, metastore: MetastoreClient, storage: StorageClient, read_headers: bool = False):
        self.table_name = table_name
        self.database_name = database_name
        self.metastore = metastore
        self.storage = storage
        self.read_headers = read_headers
        self.path: Path = storage.table_path(database_name, table_name)
        self.credentials: Union[MetastoreCredentials, InvalidCredentials] = metastore.credentials()

        parameters = self.credentials.to_dict()
        parameters['path'] = self.path.full_path()

        super().__init__("infer", parameters)

    def run(self, response: Response) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        table, reseponse =  self.metastore.get_table(self.database_name, self.table_name, {"read_headers": self.read_headers}, response)
        return self.running(object=table), response
