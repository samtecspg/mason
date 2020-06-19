from typing import Union
import magic
from s3fs import S3File
from fsspec.spec import AbstractBufferedFile

from mason.engines.metastore.models.schemas import json as JsonSchema, text as TextSchema, parquet as ParquetSchema
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema


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
    elif file_type == "empty":
        return EmptySchema()
    else:
        name = get_name(file)
        return InvalidSchema(f"File type not supported for file {name}")

def get_name(file: Union[S3File, AbstractBufferedFile]) -> str:
    if isinstance(file, S3File):
        name = "s3://" + file.path
    else:
        name = file.name
    return name

