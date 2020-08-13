from typing import Optional

from mason.engines.storage.models.path import Path
from mason.engines.execution.models.jobs import Job
from mason.engines.metastore.models.database import Database

class QueryJob(Job):

    def __init__(self, query_string: str, database: Database, output_path: Optional[Path]):
        super().__init__("query")
        self.query_string = query_string
        self.database = database
        self.output_path = output_path
        
    def spec(self) -> dict:
        spec = {
            "query_string": self.query_string,
            "database": self.database.name,
            "output_path": self.output_path.full_path()
        }
        
        if self.output_path:
            spec["output_path"] = self.output_path.full_path()
            
        return spec




