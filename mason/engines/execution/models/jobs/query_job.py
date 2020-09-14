from typing import Optional

from mason.engines.metastore.models.table import Table
from mason.engines.storage.models.path import Path
from mason.engines.execution.models.jobs import Job

class QueryJob(Job):

    def __init__(self, query_string: str, table: Table, output_path: Optional[Path] = None):
        super().__init__("query")
        self.query_string = query_string
        self.table = table
        self.output_path = output_path or Path("")
        
    def spec(self) -> dict:
        spec = {
            "input_paths": list(map(lambda p: p.full_path(), self.table.paths)),
            "input_format": self.table.schema.type,
            "query_string": self.query_string,
        }
        
        if self.output_path and self.output_path.full_path() != "":
            spec["output_path"] = self.output_path.full_path()
            
        return spec




