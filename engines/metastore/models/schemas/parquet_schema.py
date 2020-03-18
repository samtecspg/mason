
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from fastparquet import ParquetFile #type: ignore
from fastparquet.schema import SchemaHelper, schema_to_text #type: ignore

def from_footer(footer: str):
    schema: SchemaHelper = ParquetFile(footer).schema
    return ParquetSchema(schema)

class ParquetSchema(MetastoreSchema):

    def __init__(self, schema: SchemaHelper):
        # elements = schema
        # schema_elements = {k: print(v.thrift_spec) and v for k, v in elements.items() if not k == "schema"}
        self.schema = schema

