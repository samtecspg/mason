from botocore.errorfactory import ClientError
from typing import Optional, List, Union
import s3fs
from s3fs import S3FileSystem

from mason.clients.aws_client import AWSClient
from mason.clients.response import Response
from mason.engines.metastore.models.table import InvalidTable, Table
from mason.engines.storage.models.path import Path
from mason.util.json_schema import sequence
from mason.util.logger import logger
from mason.engines.metastore.models.schemas import schemas
from mason.engines.metastore.models.schemas import check_schemas as CheckSchemas
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema
from mason.util.list import get

class S3Client(AWSClient):
    def __init__(self, s3_config: dict):
        super().__init__(**s3_config)

    def client(self) -> S3FileSystem:

        return s3fs.S3FileSystem(key=self.access_key, secret=self.secret_key, client_kwargs={'region_name': self.aws_region})

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

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        return self.infer_table(database_name + "/" + table_name, table_name, options)

    def get_path(self, path: str) -> Path:
        return Path(path)

    def get_name(self, path: str):
        return path.split("/")[-1]

    def infer_table(self, path: str, name: Optional[str], options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        logger.info(f"Fetching keys at {path}")
        keys = self.client().find(path)

        if len(keys) > 0:
            valid, invalid = sequence(list(map(lambda key: schemas.from_file(self.client().open(key), options or {}), keys)), Schema, InvalidSchema)
            validated = CheckSchemas.find_conflicts(list(set(valid)))
            table = CheckSchemas.get_table(name or self.get_name(path), validated)
            invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid))
            if isinstance(table, Table):
                return table
            else:
                invalid_tables.append(table)
                return invalid_tables
        else:
            return InvalidTable(f"No keys at {path}")


    def path(self, path: str):
        if not path[0:4] == "s3://":
            path = "s3://" + path

        return Path(path)

