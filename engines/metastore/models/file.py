import magic # type: ignore
from pyarrow.parquet import ParquetFile as ArrowParquetFile # type: ignore
from abc import abstractmethod
from typing import Optional

class MetastoreFile:

    def __init__(self, file_name: str):
        self.name: str = file_name
        self.type: Optional[str] = magic.from_file(file_name)
        self.schema = self.get_schema().infer(self.name)

    def get_schema(self):
        if self.type == "Apache Parquet":
            return ParquetSchema()
        else:
            return NullSchema()

    def str(self):
        return f"NAME {self.name} TYPE {self.type} SCHEMA {self.schema}"

class MetastoreSchema:

    @abstractmethod
    def infer(self, file_name: str):
        raise NotImplementedError("Schema Inference not defined for file type")

class NullSchema(MetastoreSchema):

    def infer(self, file_name: str):
        raise NotImplementedError("Schema Inference not defined for file type")

class ParquetSchema(MetastoreSchema):

    def infer(self, file_name: str):
        return ArrowParquetFile(file_name).metadata
