from typing import Optional, List

from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path
from mason.engines.execution.models.jobs import Job

class FormatJob(Job):

    def __init__(self, table: Table, output_path: Path, format: str, partition_columns: Optional[str], filter_columns: Optional[str], partitions: Optional[str], credentials: MetastoreCredentials):
        self.table: Table = table
        self.output_path: Path = output_path
        self.format: str = format
        
        pc: List[str]
        if partition_columns:
            pc = partition_columns.split(",")
        else:
            pc = []
        self.partition_columns = pc
        
        fc: List[str]
        if filter_columns:
            fc = filter_columns.split(",")
        else:
            fc = []
        self.filter_columns = fc
        self.partitions = partitions
        self.credentials = credentials

        parameters = {
            'input_paths': list(map(lambda p: p.full_path(), self.table.paths)),
            'input_format': self.table.schema.type,
            'output_format': self.format,
            'line_terminator': self.table.line_terminator or "",
            'output_path': self.output_path.full_path(),
            'partition_columns': self.partition_columns,
            'filter_columns': self.filter_columns,
            'credentials': self.credentials.to_dict()
        }

        if self.partitions:
            parameters['partitions'] = self.partitions

        super().__init__("format")
        
    def spec(self) -> dict:

        parameters = {
            'input_paths': list(map(lambda p: p.full_path(), self.table.paths)),
            'input_format': self.table.schema.type,
            'output_format': self.format,
            'line_terminator': self.table.line_terminator or "",
            'output_path': self.output_path.full_path(),
            'partition_columns': self.partition_columns,
            'filter_columns': self.filter_columns,
            'credentials': self.credentials.to_dict()
        }

        if self.partitions:
            parameters['partitions'] = self.partitions
            
        return parameters

