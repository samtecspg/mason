
from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.storage.models.path import Path

class SummaryJob(Job):
    
    def __init__(self, input_path: Path, output_path: Path, credentials: MetastoreCredentials, read_headers: bool = False):
        self.read_headers = read_headers
        self.input_path = input_path
        self.output_path = output_path
        self.credentials = credentials

        super().__init__("summary")

    def spec(self) -> dict:
        parameters = self.credentials.to_dict()
        parameters['input_path'] = self.input_path.full_path()
        parameters['output_path'] = self.output_path.full_path()
        parameters["read_headers"] = self.read_headers

        return parameters
