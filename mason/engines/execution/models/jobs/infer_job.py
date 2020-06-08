from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.database import Database
from mason.engines.storage.models.path import Path

class InferJob(Job):

    def __init__(self, database: Database, path: Path, credentials: MetastoreCredentials):
        self.database = database
        self.path = path
        self.credentials = credentials

        super().__init__("infer", self.credentials.to_dict())



