from typing import Union

import magic
from s3fs import S3File

from clients.response import Response
from engines.metastore.models.schemas.schema import emptySchema, InvalidSchema

from engines.metastore.models.schemas import parquet as ParquetSchema
from engines.metastore.models.schemas import json as JsonSchema
from engines.metastore.models.schemas import text as TextSchema
from engines.metastore.models.schemas.schema import Schema
from fsspec.spec import AbstractBufferedFile

def header_length(file: AbstractBufferedFile):
    head = []
    for i in range(4):
        head.append(file.readline().decode("utf-8"))
    file.seek(0)
    header_length = max(list(map(lambda l: len(l.split(",")), head)))
    return header_length


def from_file(file: AbstractBufferedFile, options: dict = {}) -> Union[Schema, InvalidSchema]:
    header_size = 1000
    header = file.read(header_size)
    file_type = magic.from_buffer(header)
    file.seek(0)

    if file_type == "Apache Parquet":
        return ParquetSchema.from_file(file)
    elif file_type == "JSON data":
        return JsonSchema.from_file(get_name(file))
    elif file_type == "CSV text" or file_type == "ASCII text":
        return TextSchema.from_file(get_name(file), "none", header_length(file), options.get("read_headers"))
    else:
        name = get_name(file)
        return InvalidSchema(f"File type not supported for file {name}")

def get_name(file: Union[S3File, AbstractBufferedFile]) -> str:
    if isinstance(file, S3File):
        name = "s3://" + file.path
    else:
        name = file.name
    return name

