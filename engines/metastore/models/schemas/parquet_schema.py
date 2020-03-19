
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, SchemaElement
from fastparquet import ParquetFile #type: ignore
from fastparquet.schema import SchemaHelper #type: ignore
from util.list import get, flatten
from typing import Optional, List

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

    def __init__(self, columns: List[SchemaElement], schema_helper: Optional[SchemaHelper] = None):
        self._schema = schema_helper
        self.columns = columns
        self.type = 'parquet'


def from_footer(footer: str):
    schema: SchemaHelper = ParquetFile(footer).schema

    # TODO:  Find a more graceful way to do this than parsing the text
    text = schema.text
    split = text.split("\n")
    split.pop(0)
    column_elements: List[SchemaElement] = flatten(list(map(lambda line: element_from_text(line), split)))

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
