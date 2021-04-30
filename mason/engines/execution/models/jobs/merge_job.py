from typing import Optional, Any, Dict

from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.storage.models.path import Path

class MergeJob(Job):

    def __init__(self, input_format: str, input_path: Path, output_path: Path, credentials: Optional[MetastoreCredentials] = None, read_headers: bool = False):
        self.input_format = input_format
        self.input_path = input_path
        self.output_path = output_path
        self.credentials = credentials
        self.read_headers = read_headers
        super().__init__("merge")
        
    def spec(self) -> dict:
        spec: Dict[str, Any] = {
            'input_path': self.input_path.full_path(), 
            'output_path': self.output_path.full_path(), 
            'input_format': self.input_format
        }
        credentials = self.credentials
        if credentials:
            spec = {**spec, **credentials.to_dict()}
            
        spec['read_headers'] = self.read_headers
        
        return spec
            


