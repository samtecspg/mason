from typing import List, Union
import json
import fsspec
import jsonlines
import os
from genson import SchemaBuilder

from mason.engines.metastore.models.schemas.schema import InvalidSchema, Schema
from mason.engines.metastore.models.schemas.schema import SchemaElement
from mason.util.exception import message

def from_file(file: str):
    # TODO: large json files
    if os.path.splitext(file)[1] == ".jsonl":
        builder = SchemaBuilder()
        with jsonlines.open(file, 'r') as reader:
            for data in reader.iter(type=dict, skip_invalid=True):
                builder.add_object(data)
        schema = builder.to_schema()
        return JsonSchema(schema, "jsonl")
    else:
        try:
            with fsspec.open(file) as f:
                data = json.load(f)
                builder = SchemaBuilder()
                builder.add_object(data)
                schema = builder.to_schema()
                return JsonSchema(schema)
        except FileNotFoundError as e:
            return InvalidSchema(f"File not found {file}")


class JsonSchema(Schema):

    def __init__(self, schema: dict, type: str = "json"):
        self.schema = schema
        self.type = type
        self.columns: List[SchemaElement] =  [] # TODO:  Treating json data as non tabular for now.   Surface main columns and nested attributes


def merge_json_schemas(schemas: List[Schema]) -> Union[JsonSchema, InvalidSchema]:
    try:
        builder = SchemaBuilder()
        for schema in schemas:
            if isinstance(schema, JsonSchema):
                builder.add_schema(schema.schema)
            else:
                return InvalidSchema("merge_json_schemas Only supports JsonSchema type")
        merged = builder.to_schema()
        return JsonSchema(merged)
    except Exception as e:
        return InvalidSchema(f"Invalid Schema, builder error: {message(e)}")









