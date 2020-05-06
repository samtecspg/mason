from util.uuid import uuid4

from engines.execution.models.jobs import Job


class InferJob(Job):

    def __init__(self, database_name: str, storage_path: str):
        job_id = uuid4()
        super().__init__(job_id)
        self.type = "infer"
        self.database_name = database_name
        self.storage_path = storage_path
