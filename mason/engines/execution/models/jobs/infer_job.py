
from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.storage.models.path import Path

class InferJob(Job):

    def __init__(self, database_name: str, table_name: str, path: Path, credentials: MetastoreCredentials, read_headers: bool = False):
        self.table_name = table_name
        self.database_name = database_name
        self.read_headers = read_headers
        self.credentials = credentials
        self.path = path
        super().__init__("infer")
        
    def spec(self) -> dict:
        parameters = self.credentials.to_dict()
        parameters['path'] = self.path.full_path()
        return parameters

        

