
from engines.execution.models.jobs import Job
from engines.metastore.models.database import Database
from engines.storage import Path

class InferJob(Job):

    def __init__(self, database: Database, path: Path):
        self.database = database
        self.path = path
