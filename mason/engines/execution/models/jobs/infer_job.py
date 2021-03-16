
from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.storage.models.path import Path

class InferJob(Job):

    def __init__(self, database_name: str, table_name: str, path: Path, credentials: MetastoreCredentials, read_headers: bool = False):
        self.table_name = table_name
        self.database_name = database_name
        self.read_headers = read_headers

        parameters = credentials.to_dict()
        parameters['path'] = path.full_path()

        super().__init__("infer", parameters)
