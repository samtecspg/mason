from typing import List, Tuple

from fsspec.spec import AbstractBufferedFile

from clients.response import Response
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, emptySchema
from genson import SchemaBuilder
import json
import fsspec
import jsonlines
import os

def from_file(file: str, response: Response):
    # TODO: large json files
    if os.path.splitext(file)[1] == ".jsonl":
        builder = SchemaBuilder()
        with jsonlines.open(file, 'r') as reader:
            for data in reader.iter(type=dict, skip_invalid=True):
                builder.add_object(data)
        schema = builder.to_schema()
        return response, JsonSchema(schema, "jsonl")
    else:
        try:
            with fsspec.open(file) as f:
                data = json.load(f)
                builder = SchemaBuilder()
                builder.add_object(data)
                schema = builder.to_schema()
                return response, JsonSchema(schema)
        except FileNotFoundError as e:
            response.add_error(f"File not found {file}")
            return response, emptySchema()


class JsonSchema(MetastoreSchema):

    def __init__(self, schema: dict, type: str = "json"):
        self.schema = schema
        self.type = type
        self.columns =  [] # TODO:  Treating json data as non tabular for now.   Surface main columns and nested attributes


def merge_schemas(schemas: List[JsonSchema], response: Response) -> Tuple[Response, MetastoreSchema]:
    builder = SchemaBuilder()
    for schema in schemas:
        builder.add_schema(schema.schema)

    return response, JsonSchema(builder.to_schema())




