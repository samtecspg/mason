from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.database import Database


class QueryJob(Job):

    def __init__(self, query_string: str, database: Database):
        super().__init__("query")
        self.query_string = query_string
        self.database = database




