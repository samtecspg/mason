from typing import Optional, Union

from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.database import Database
from mason.engines.storage.models.path import Path

class SummaryJob(Job):

    def __init__(self, path: Path, credentials: Union[MetastoreCredentials, InvalidCredentials], database: Optional[Database] = None):
        self.path = path
        self.credentials = credentials

        parameters = self.credentials.to_dict()
        parameters['path'] = path.full_path()

        super().__init__("summary", parameters)
