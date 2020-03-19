
import boto3 # type: ignore
from clients.response import Response
from botocore.errorfactory import ClientError # type: ignore
from typing import Optional, List
from util.logger import logger
from engines.metastore.models import schemas
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from engines.metastore.models.schemas import check_schemas as CheckSchemas
from util.list import get

class S3Client:
    def __init__(self, s3_config: dict):
        self.region = s3_config.get("region")
        self.client = boto3.client('s3', region_name=self.region)

        # self.resource = boto3.resource('s3', region_name=self.region)
        # self.client = self.resource.meta.client

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

    def get_results(self, response: Response, database_name: str, table_name: Optional[str] = None) -> Response:
        logger.info(f"Fetching keys in {database_name} {table_name}")

        new_responses: List[dict] = []

        try:
            if table_name:
                result = self.client.list_objects_v2(Bucket=database_name, Prefix=table_name)
            else:
                result = self.client.list_objects_v2(Bucket=database_name)
            new_responses.append(result)
            continuation_token = result.get('NextContinuationToken')
        except ClientError as e:
            new_responses.append(e.response)

        while continuation_token:
            try:
                logger.info(f"Continuing fetching keys in {database_name}. Continuation token: {continuation_token}, Responses: {len(new_responses)}")
                if table_name:
                    result = self.client.list_objects_v2(Bucket=database_name, Prefix=table_name)
                else:
                    result = self.client.list_objects_v2(Bucket=database_name)
                new_responses.append(result)
                continuation_token = result.get('NextContinuationToken')
            except ClientError as e:
                new_responses.append(e.response)

        for resp in new_responses:
            contents = resp.get("Contents", [])
            keys = list(map(lambda c: c.get("Key"), contents))
            schema_list: List[MetastoreSchema] = []
            if len(keys) > 0:
                for key in keys:
                    # get header to infer file type
                    header_length = 4096
                    header_response = self.client.get_object(Bucket=database_name, Key=key, Range =f'bytes=0-{header_length}')
                    response.add_response(header_response)
                    header: bytes = header_response['Body'].read()
                    object_header_response = self.client.head_object(Bucket=database_name, Key=key)
                    response.add_response(object_header_response)
                    content_length = int(object_header_response.get('ResponseMetadata', {}).get("HTTPHeaders", {}).get("content-length", "0"))
                    if content_length > 0:
                        footer_length = 20000
                        footer_start = content_length - footer_length

                        footer_response = self.client.get_object(Bucket=database_name, Key=key, Range=f"bytes={footer_start}-{content_length}")
                        response.add_response(footer_response)

                        footer: bytes = footer_response['Body'].read()
                        schema = schemas.from_header_and_footer(header, footer)
                        schema_list.append(schema)

        schemas_checked = CheckSchemas.find_conflicts(list(set(schema_list)))

        if (len(new_responses) > 0):
            error, status, message = self.parse_responses(new_responses[-1])

            if error == "EntityNotFoundException":
                response.add_error(f"Database {database_name} not found")
                response.set_status(404)
            elif 200 <= status < 300:
                response.add_data(schemas_checked)
                response.set_status(status)
            else:
                response.set_status(status)
                response.add_error(message)

        return response


    #  List tables for s3 only lists out folders, not schemas in folders.  You can specify subfolders and it will be split out
    def list_tables(self, database_name: str, response: Response):
        split = database_name.split("/", 1)
        result = self.client.list_objects(Bucket=split[0], Prefix=(get(split, 1) or '/'), Delimiter='/').get("CommonPrefixes", {})
        response.add_data(result)
        return response

    #  database_name = bucket, table_name = path
    def get_table(self, database_name: str, table_name: str, response: Response):
        response = self.get_results(response, database_name, table_name)
        return response

    def path(self, path: str):
        return "s3://" + path

    # TODO: Validate that the specified path exists before bothering other clients with it
    # def validate_path(self, path: str):
    #     try:
    #         s3 = boto3.resource('s3')
    #         object = s3.Object('bucket_name', 'key')
    #     except ClientError as e:
    #         result = e.response
