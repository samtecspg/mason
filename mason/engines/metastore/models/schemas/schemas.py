from typing import Union
import magic
from s3fs import S3File
from fsspec.spec import AbstractBufferedFile

from mason.engines.metastore.models.schemas import json as JsonSchema, text as TextSchema, parquet as ParquetSchema
from mason.engines.metastore.models.schemas.text import SUPPORTED_TYPES as supported_text_types
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema
from mason.engines.storage.models.path import Path

def header_length(file: AbstractBufferedFile):
    head = []
    for i in range(4):
        head.append(file.readline().decode("utf-8"))
    file.seek(0)
    header_length = max(list(map(lambda l: len(l.split(",")), head)))
    return header_length

def from_file(file: AbstractBufferedFile, options: dict = {}) -> Union[Schema, InvalidSchema]:
    sample_size = 10000
    sample = file.read(sample_size)
    file_type = magic.from_buffer(sample)
    
    # Note: file magic is not great, find a way to replace it 
    # TODO: Remove this hack for Debian  https://github.com/ahupp/python-magic/issues/208
    path_str = get_path(file).path_str
    if path_str.endswith(".csv") and file_type == "ASCII text":
        file_type = "CSV text"
    # TODO: Remove this hack for Ubuntu 
    elif (path_str.endswith(".json") or path_str.endswith(".jsonl")) and file_type == "ASCII text":
        file_type = "JSON data"

    file.seek(0)

    path = get_path(file)
    if file_type == "Apache Parquet":
        return ParquetSchema.from_file(file, path)
    elif file_type == "JSON data":
        return JsonSchema.from_file(path)
    elif file_type in list(supported_text_types.keys()):
        return TextSchema.from_file(file_type, sample, path, options.get("read_headers"))
    elif file_type == "empty":
        return EmptySchema()
    else:
        return InvalidSchema(f"File type not supported for file {path.path_str}.  Type: {file_type}")

def get_path(file: Union[S3File, AbstractBufferedFile]) -> Path:
    if isinstance(file, S3File):
        path = Path(file.path, "s3")
    else:
        path = Path(file.name)
    return path

