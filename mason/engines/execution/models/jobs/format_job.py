
from mason.engines.storage.models.path import Path
from mason.engines.metastore.models.table import Table
from mason.engines.execution.models.jobs import Job

class FormatJob(Job):

    def __init__(self, table: Table, output_path: Path, format: str):
        self.table = table
        self.output_path = output_path
        self.format = format
        super().__init__("format")
