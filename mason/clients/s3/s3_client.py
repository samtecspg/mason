import re

from botocore.errorfactory import ClientError
from typing import Optional, List, Union, Tuple
import s3fs
from s3fs import S3FileSystem

from mason.clients.aws_client import AWSClient
from mason.clients.response import Response
from mason.engines.metastore.models.table import InvalidTable, Table, TableNotFound, InvalidTables
from mason.engines.storage.models.path import Path
from mason.util.exception import message
from mason.util.logger import logger
from mason.engines.metastore.models.schemas import schemas
from mason.engines.metastore.models.schemas import check_schemas as CheckSchemas
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema
from mason.util.list import get, sequence


class S3Client(AWSClient):
    def __init__(self, s3_config: dict):
        super().__init__(**s3_config)

    def client(self) -> S3FileSystem:
        s3 = s3fs.S3FileSystem(key=self.access_key, secret=self.secret_key, client_kwargs={'region_name': self.aws_region})

        return s3

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

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        return self.infer_table(database_name + "/" + table_name, table_name, options, response)

    def get_path(self, path: str) -> Path:
        return Path(path, "s3")

    def get_name(self, name: Optional[str], path: str) -> str:
        if not name or name == "":
            return path.rstrip("/").split("/")[-1]
        else:
            return name

    def infer_table(self, path: str, name: Optional[str], options: Optional[dict] = None, resp: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        logger.info(f"Fetching keys at {path}")
        response: Response = resp or Response()
        
        keys, response = self.list_keys(path, response)
        
        final: Union[Table, InvalidTables]

        if len(keys) > 0:
            valid, invalid_schemas = sequence(list(map(lambda key: schemas.from_file(self.client().open(key.full_path()), options or {}), keys)), Schema, InvalidSchema)
            non_empty = [v for v in valid if not isinstance(v, EmptySchema)]
            validated = CheckSchemas.find_conflicts(list(set(non_empty)))
            table = CheckSchemas.get_table(self.get_name(name, path), validated)
            invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid_schemas))
            if isinstance(table, Table):
                final = table
            else:
                invalid_tables.append(table)
                final = InvalidTables(invalid_tables)
        else:
            response.set_status(404)
            final = InvalidTables([TableNotFound(f"No keys at {path}")])
            
        return final, response
    
    def list_keys(self, path: str,  response: Optional[Response] = None) -> Tuple[List[Path], Response]:
        resp: Response = response or Response()
        keys = self.client().find(path)
        if len(keys) > 0:
            paths = list(map(lambda k: self.get_path(k), keys))
        else:
            paths = []
        
        return paths, resp

    def path(self, path: str):
        if not path[0:4] == "s3://":
            path = "s3://" + path
            
        return Path(path, "s3")


def save_to(self, inpath: Path, outpath: Path, response: Response):
        try:
            self.client().upload(inpath.path_str, outpath.path_str)
        except Exception as e:
            response.add_error(f"Error saving {inpath} to {outpath.path_str}")
            response.add_error(message(e))
            
        return response
        
        
