from engines.execution.models.jobs import Job

class QueryJob(Job):

    def __init__(self, query_string: str, database_name: str, params: dict):
        super().__init__("query", {'query_string': query_string, 'database_name': database_name}.update(params))


