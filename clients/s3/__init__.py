
import boto3 # type: ignore
from clients.response import Response
from botocore.errorfactory import ClientError # type: ignore
from typing import Optional, List, Tuple
from util.logger import logger
from engines.metastore.models import schemas
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from engines.metastore.models.schemas import check_schemas as CheckSchemas
from util.list import get
import s3fs #type: ignore

class S3Client:
    def __init__(self, s3_config: dict):
        self.region = s3_config.get("region")
        self.client = boto3.client('s3', region_name=self.region)

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

    def get_results(self, response: Response, database_name: str, table_name: Optional[str] = None) -> Tuple[List[MetastoreSchema], Response]:
        logger.info(f"Fetching keys in {database_name} {table_name}")

        s3 = s3fs.S3FileSystem()
        keys = s3.find(database_name + "/" + (table_name or ""))

        schema_list: List[MetastoreSchema] = []
        if len(keys) > 0:
            for key in keys:
                logger.debug(f"Key {key}")
                k = s3.open(key)
                response, schema = schemas.from_file(k, response)
                if schema:
                    schema_list.append(schema)

        schema_listing, schema_data = CheckSchemas.find_conflicts(list(set(schema_list)))
        response.add_data(schema_data)

        return schema_listing, response


    #  List tables for s3 only lists out folders, not schemas in folders.  You can specify subfolders and it will be split out
    def list_tables(self, database_name: str, response: Response):
        split = database_name.split("/", 1)
        try:
            result = self.client.list_objects(Bucket=split[0], Prefix=(get(split, 1) or '/'), Delimiter='/')
            response.add_data(result.get("CommonPrefixes", {}))
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

    def get_table(self, database_name: str, table_name: str, response: Response) -> Tuple[List[MetastoreSchema], Response]:
       return self.get_results(response, database_name, table_name)

    def path(self, path: str):
        return "s3://" + path

    # TODO: Validate that the specified path exists before bothering other clients with it
    # def validate_path(self, path: str):
    #     try:
    #         s3 = boto3.resource('s3')
    #         object = s3.Object('bucket_name', 'key')
    #     except ClientError as e:
    #         result = e.response
