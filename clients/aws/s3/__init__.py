from clients.response import Response
from botocore.errorfactory import ClientError
from typing import Optional, List, Tuple, Union

from engines.metastore.models.table import Table, InvalidTable
from engines.storage.models.path import Path
from util.json_schema import sequence
from util.logger import logger
from engines.metastore.models import schemas
from engines.metastore.models.schemas.schema import Schema, InvalidSchema, SchemaConflict
from engines.metastore.models.schemas import check_schemas as CheckSchemas
from util.list import get
import s3fs
from s3fs import S3FileSystem
from fsspec.spec import AbstractBufferedFile

class S3Client:
    def __init__(self, s3_config: dict):
        self.region = s3_config.get("aws_region")

    def client(self) -> S3FileSystem:
        return s3fs.S3FileSystem(client_kwargs={'region_name': self.region})

    def parse_responses(self, s3_response: dict):
        error = s3_response.get('Error', {}).get('Code', '')
        status = s3_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = s3_response.get('Error', {}).get('Message')
        return error, status, message


    def parse_table_list_data(self, s3_response: dict):
        single_items = list(map(lambda x: self.parse_item(x), s3_response.get('Contents', [])))
        prefixes = list(map(lambda x: self.parse_prefixes(x), s3_response.get('CommonPrefixes', [])))
        return {
            "items": single_items,
            "prefixes": prefixes
        }

    def parse_prefixes(self, s3_response: dict):
        return s3_response.get("Prefix")

    def parse_item(self, s3_response: dict):
        table_parsed = {
            "name": s3_response.get("Key"),
            "updated_at": s3_response.get("LastModified"),
            "size": s3_response.get("Size")
        }
        return table_parsed

    def parse_items(self, s3_response: dict):
        return list(map(lambda x: self.parse_item(x), s3_response.get('Contents', [])))

    def get_results(self, response: Response, database_name: str, table_name: Optional[str] = None, options: dict = {}) -> Tuple[List[Schema], Response]:
        logger.info(f"Fetching keys in {database_name} {table_name}")

        keys = self.client().find(database_name + "/" + (table_name or ""))

        schema_list: List[Schema] = []
        if len(keys) > 0:
            for key in keys:
                logger.debug(f"Key {key}")
                k = self.client().open(key)
                schema = schemas.from_file(k, options)
                if isinstance(schema, Schema):
                    schema_list.append(schema)
                else:
                    response.add_error(schema.reason)

        validated = CheckSchemas.find_conflicts(list(set(schema_list)), response)

        if validated:
            response.add_data(validated.to_dict())



    #  List tables for s3 only lists out folders, not schemas in folders.  You can specify subfolders and it will be split out
    def list_tables(self, database_name: str, response: Response):
        split = database_name.split("/", 1)
        try:
            result = self.client().s3.list_objects(Bucket=split[0], Prefix=(get(split, 1) or '/'), Delimiter='/')
            response.add_data({"Prefixes": result.get("CommonPrefixes", {})})
        except ClientError as e:
            result = e.response
            error = result.get("Error", {})
            code = error.get("Code", "")
            if code == "NoSuchBucket":
                response.add_error(error.get("Message") + f": {database_name}")
                response.set_status(404)
            else:
                raise e

        response.add_response(result)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[Schema], Response]:
        return self.get_results(response, database_name, table_name, options)

    def get_path(self, path: str) -> Path:
        return Path(path)

    def get_schemas(self, key: str, options: dict) -> Tuple[Schema, InvalidSchema]:
        opened = self.client().open(key)
        if isinstance(opened, AbstractBufferedFile):
            schemas.from_file(k, options)

    def infer_table(self, name: str, path: str, options: dict = {}) -> Tuple[Optional[Table], List[InvalidTable]]:
        logger.info(f"Fetching keys at {path}")
        keys = self.client().find(path)

        if len(keys) > 0:
            valid, invalid = sequence(list(map(lambda key: schemas.from_file(self.client().open(key), options), keys)), Schema, InvalidSchema)
            validated = CheckSchemas.find_conflicts(list(set(valid)))
            table = CheckSchemas.get_table(name, validated)
            invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid))
            if isinstance(table, Table):
                return table, invalid_tables
            else:
                invalid_tables.append(table)
                return None, invalid_tables
        else:
            return None, [InvalidTable(f"No keys at {path}")]


    def path(self, path: str):
        return "s3://" + path

