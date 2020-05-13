from clients.response import Response
from util.uuid import uuid4

from engines.execution.models.jobs import Job

class InferJob(Job):

    def __init__(self, database_name: str, storage_path: str):
        self.type = "infer"
        self.database_name = database_name
        self.storage_path = storage_path

    def run(self, response: Response):
        job_id = uuid4()
        super().__init__(job_id)
        return job_id, response
