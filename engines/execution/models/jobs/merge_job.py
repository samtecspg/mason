from engines.execution.models.jobs import Job


class MergeJob(Job):

    def __init__(self, input_path: str, output_path: str, input_format: str, params: dict):
        super().__init__("query", {'input_path':input_path, 'output_path': output_path, 'input_format': input_format}.update(params))


