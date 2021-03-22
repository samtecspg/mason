
from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.storage.models.path import Path

class SummaryJob(Job):
    
    def __init__(self, input_path: Path, output_path: Path, credentials: MetastoreCredentials, read_headers: bool = False):
        self.read_headers = read_headers

        parameters = credentials.to_dict()
        parameters['input_path'] = input_path.full_path()
        parameters['output_path'] = output_path.full_path()
        parameters['read_headers'] = read_headers

        super().__init__("summary", parameters)
