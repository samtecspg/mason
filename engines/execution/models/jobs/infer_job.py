from engines.metastore.models.credentials import MetastoreCredentials

from engines.execution.models.jobs import Job
from engines.metastore.models.database import Database
from engines.storage.models.path import Path

class InferJob(Job):

    def __init__(self, database: Database, path: Path, credentials: MetastoreCredentials):
        self.database = database
        self.path = path
        self.credentials = credentials

        super().__init__("infer", self.credentials.to_dict())



