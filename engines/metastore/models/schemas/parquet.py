
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, SchemaElement
from fastparquet import ParquetFile
from fastparquet.schema import SchemaHelper
from util.list import get, flatten
from typing import Optional, List
from fsspec.spec import AbstractBufferedFile
from typing import Union

class ParquetElement(SchemaElement):
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

class ParquetSchema(MetastoreSchema):

    def __init__(self, columns: List[ParquetElement], schema_helper: Optional[SchemaHelper] = None):
        self._schema = schema_helper
        self.columns = columns
        self.type = 'parquet'

def from_file(file: Union[str, AbstractBufferedFile]):
    schema: SchemaHelper = ParquetFile(file).schema
    return schema_from_text(schema)

def schema_from_text(schema: SchemaHelper):
    text = schema.text
    split = text.split("\n")
    split.pop(0)
    column_elements: List[ParquetElement] = flatten(list(map(lambda line: element_from_text(line), split)))
    return ParquetSchema(column_elements, schema)

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
