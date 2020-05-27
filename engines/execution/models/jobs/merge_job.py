from engines.execution.models.jobs import Job
from engines.storage.models.path import Path


class MergeJob(Job):

    def __init__(self, input_path: Path, output_path: Path, input_format: str):
        super().__init__("merge", {'input_path': input_path.path_str, 'output_path': output_path.path_str, 'input_format': input_format})

