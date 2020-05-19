from typing import Tuple

import boto3
from botocore.client import BaseClient

from clients.response import Response
from botocore.errorfactory import ClientError
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, SchemaElement

class GlueClient:

    def __init__(self, config: dict):
        self.aws_role_arn = config.get("aws_role_arn")
        self.aws_region = config.get("aws_region")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")

    def client(self) -> BaseClient:
        return boto3.client('glue', region_name=self.aws_region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

    def list_tables(self, database_name: str, response: Response):
        try:
            result = self.client().get_tables(DatabaseName=database_name)
        except ClientError as e:
            result = e.response
        response.add_response(result)
        error, status, message = self.parse_response(result)

        if error == "EntityNotFoundException":
            response.add_error(f"Database {database_name} not found")
            response.set_status(404)
        elif 200 <= status < 300:
            data = {'Tables': self.parse_table_list_data(result)}
            response.add_data(data)
            response.set_status(status)
        else:
            response.set_status(status)
            response.add_error(message)

        return response


    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        try:
            glue_response = self.client().delete_table(
                DatabaseName=database_name,
                Name=table_name
            )

        except ClientError as e:
            glue_response = e.response

        error, status, message = self.parse_response(glue_response)
        response.add_response(glue_response)

        if not error == "":
            response.set_status(status)
            response.add_error(message)

        else:
            response.add_info(f"Table {table_name} successfully deleted.")

        return response

    def register_schedule(self, database_name: str, path: str, schedule_name: str, response: Response):
        create_crawler_response = self.create_glue_crawler(
            database=database_name,
            name=schedule_name,
            role=self.aws_role_arn or "",
            path=path
        )

        response.add_response(create_crawler_response)
        error, status, message = self.parse_response(create_crawler_response)

        if error == "AlreadyExistsException":
            response.add_warning(f"Table crawler {schedule_name} already exists. Skipping creation.")
            response.set_status(201)
        elif error == "CrawlerRunningException":
            response.add_warning(f"Table crawler {schedule_name} is already refreshing.")
            response.set_status(202)
        elif 200 <= status < 300:
            response.add_info(f"Created table crawler {schedule_name}.")
            response.set_status(201)
        else:
            response.add_error(message)
            response.set_status(status)

        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        try:
            glue_response = self.client().delete_crawler(
                Name=schedule_name
            )

        except ClientError as e:
            glue_response = e.response

        error, status, message = self.parse_response(glue_response)
        response.add_response(glue_response)

        if not error == "":
            response.set_status(status)
            response.add_error(message)

        else:
            response.add_info(f"Schedule {schedule_name} successfully deleted.")

        return response

    def trigger_schedule(self, schedule_name: str, response: Response):
        refresh_glue_table_response = self.refresh_glue_table(schedule_name)
        error, status, message = self.parse_response(refresh_glue_table_response)

        response.add_response(refresh_glue_table_response)

        if error == "CrawlerRunningException":
            response.add_warning(f"Table crawler {schedule_name} is already refreshing.")
            response.add_data({})
            response.set_status(202)
        elif status:
            if 200 <= status < 300:
                response.add_info(f"Refreshing Table Crawler: {schedule_name}")
                response.add_data({})
                response.set_status(201)
        else:
            response.add_error(message)
            response.set_status(status)
        return response

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response):
        schema, get_glue_table_response = self.get_table(database_name, table_name, response)

        crawler_name = None
        if 200 <= get_glue_table_response.status_code < 300:
            crawler_name = get_glue_table_response.responses[-1].get("Table", {}).get("Parameters", {}).get("UPDATED_BY_CRAWLER")

        if crawler_name:
            self.trigger_schedule(crawler_name, response)
        else:
            response.add_error("Could not find crawler for table")
            response.set_status(404)

        return response

    def get_table(self, database_name: str, table_name: str, response: Response) -> Tuple[MetastoreSchema, Response]:

        try:
            result = self.client().get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            result = e.response

        response.add_response(result)
        error, status, message = self.parse_response(result)
        data, schema = self.parse_table_data(result.get("Table", {}))

        if error == "EntityNotFoundException":
            response.add_error(f"Database {database_name} or table {table_name} not found")
            response.set_status(404)
        elif 200 <= status < 300:
            response.add_data(data)
            response.set_status(status)
        else:
            response.add_error(message)
            response.set_status(status)

        return schema, response


    def refresh_glue_table(self, crawler_name: str):
        try:
            result = self.client().start_crawler(Name=crawler_name)
        except ClientError as e:
            result = e.response
        return result

    def create_glue_crawler(self, database: str, name: str, role: str, path: str):
        targets = {
            'S3Targets': [
                {
                    'Path': path,
                    'Exclusions': []
                }
            ]
        }

        try:
            result = self.client().create_crawler(
                DatabaseName=database,
                Name=name,
                Role=role,
                Targets=targets
            )
        except ClientError as e:
            result = e.response

        return result

    def parse_response(self, glue_response: dict):
        error = glue_response.get('Error', {}).get('Code', '')
        status = glue_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = glue_response.get('Error', {}).get('Message')
        return error, status, message

    def parse_table_data(self, glue_response: dict):
        columns_response = glue_response.get("StorageDescriptor", {}).get("Columns")
        if columns_response:
            columns = list(map(lambda c: SchemaElement(c.get("Name"), c.get("Type")), columns_response))
        else:
            columns = []

        schema = MetastoreSchema(columns, "glue")

        table_parsed = {
            "Name": glue_response.get("Name"),
            "CreatedAt": glue_response.get("CreateTime"),
            "CreatedBy": glue_response.get("CreatedBy"),
            "DatabaseName": glue_response.get("DatabaseName"),
            "Schema": schema.to_dict(),

        }
        return table_parsed, schema

    def parse_table_list_data(self, glue_response: dict):
        return list(map(lambda x: self.parse_table_data(x)[0], glue_response['TableList']))


