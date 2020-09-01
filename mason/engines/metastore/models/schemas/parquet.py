from io import BufferedReader

from fsspec.implementations.local import LocalFileOpener

from fastparquet import ParquetFile
from fastparquet.schema import SchemaHelper

from mason.engines.metastore.models.schemas.schema import SchemaElement, InvalidSchema, Schema
from mason.engines.storage.models.path import Path
from mason.util.list import get, flatten
from typing import Optional, List
from fsspec.spec import AbstractBufferedFile
from typing import Union

class ParquetElement(SchemaElement):
    PD_TYPE_MAPPING = {
        "DOUBLE": "float64",
        "BOOLEAN": "bool",
        "INT32": "int32",
        "INT64": "int64",
        "FLOAT": "float32",
        "BYTE_ARRAY": "object"
    }

    def __init__(self, name: str, type: str, converted_type: Optional[str], repitition_type: Optional[str]):
        self.converted_type = converted_type
        self.repitition_type = repitition_type
        super().__init__(name, type)

    def __eq__(self, other):
        if other:
            return (self.name == other.name and self.type == other.type and self.converted_type == other.converted_type and self.repitition_type == other.repitition_type)
        else:
            False

    def __hash__(self):
        return 0


    def to_dict(self):
        return {
            'Name': self.name,
            'Type': self.type,
            'ConvertedType': self.converted_type,
            'RepititionType': self.repitition_type
        }

    def to_pd_dict(self) -> dict:
        return {self.name: self.lookup_pd_type(self.type)}

    def lookup_pd_type(self, type: str) -> str:
        return self.PD_TYPE_MAPPING.get(type, "object")

class ParquetSchema(Schema):

    def __init__(self, columns: List[ParquetElement], path: Path, schema_helper: Optional[SchemaHelper] = None):
        self._schema = schema_helper
        super().__init__(columns, 'parquet', path)

def from_file(file: Union[AbstractBufferedFile, BufferedReader, LocalFileOpener], path: Path) -> Union[ParquetSchema, InvalidSchema]:
    if isinstance(file, AbstractBufferedFile) or isinstance(file, BufferedReader) or isinstance(file, LocalFileOpener):
        try:
            schema: SchemaHelper = ParquetFile(file).schema
            return schema_from_text(schema, path)
        except ValueError as e:
            from mason.util.exception import message
            return InvalidSchema(f"Invalid Schema: {message(e)}")
    else:
        return InvalidSchema(f"Invalid Schema {file}")

def schema_from_text(schema: SchemaHelper, path: Path) -> ParquetSchema:
    text = schema.text
    split = text.split("\n")
    split.pop(0)
    column_elements: List[ParquetElement] = flatten(list(map(lambda line: element_from_text(line), split)))
    return ParquetSchema(column_elements, path, schema)

def element_from_text(line: str) -> Optional[ParquetElement]:
    split = line.lstrip("| - ").split(":")
    name: Optional[str] = get(split, 0)
    attributes: Optional[str] = get(split, 1)
    attrs_split = (attributes or "").replace(" ", "").split(",")
    type = get(attrs_split, 0)
    converted_type = get(attrs_split, 1)
    repitition_type = get(attrs_split, 2)
    if name and type:
        return ParquetElement(name, type, converted_type, repitition_type)
    else:
        return None
