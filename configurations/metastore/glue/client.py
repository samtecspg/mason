
from configurations.response import Response
import boto3 # type: ignore
import s3fs # type: ignore
from botocore.errorfactory import ClientError # type: ignore
from typing import Optional

class GlueMetastoreClient:

    def __init__(self, region: Optional[str]):
        self.region = region
        self.client = boto3.client('glue', region_name=self.region)

    def list_tables(self, database_name: str, response: Response):
        try:
            result = self.client.get_tables(DatabaseName=database_name)
        except ClientError as e:
            result = e.response
        response.add_response(result)
        error, status, message = self.parse_response(result)

        if error == "EntityNotFoundException":
            response.add_error(f"Database {database_name} not found")
            response.set_status(404)
        elif 200 <= status < 300:
            data = self.parse_table_list_data(result)
            response.add_data(data)
            response.set_status(status)
        else:
            response.set_status(status)
            response.add_error(message)

        return response

    # def refresh_table(self, name: str, database_name: str, response: Response, crawler_name: Optional[str] = None):
    def refresh_table(self, name: str, database_name: str, response: Response):

        # if crawler_name == None:
        crawler_name = None
        get_glue_table_response = self.get_table(database_name, name, response)
        if 200 <= get_glue_table_response.status_code < 300:
            crawler_name = get_glue_table_response.responses[-1].get("Table", {}).get("Parameters", {}).get("UPDATED_BY_CRAWLER")

        if crawler_name:
            refresh_glue_table_response = self.refresh_glue_table(crawler_name)
            error, status, message = self.parse_response(refresh_glue_table_response)

            response.add_response(refresh_glue_table_response)

            if error == "CrawlerRunningException":
                response.add_warning(f"Table crawler {crawler_name} is already refreshing.")
                response.set_status(202)
            elif status:
                if 200 <= status < 300:
                    response.add_info(f"Refreshing Table Crawler: {crawler_name}")
                    response.set_status(201)
            else:
                response.add_error(message)
                response.set_status(status)
        # else:
        #     response.add_error("Could not find crawler for table")
        #     response.set_status(404)

        return response

    def get_table(self, database_name: str, table_name: str, response: Response):

        try:
            result = self.client.get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            result = e.response

        response.add_response(result)
        error, status, message = self.parse_response(result)
        data = self.parse_table_data(result.get("Table", {}))

        if error == "EntityNotFoundException":
            response.add_error(f"Database {database_name} or table {table_name} not found")
            response.set_status(404)
        elif 200 <= status < 300:
            response.add_data(data)
            response.set_status(status)
        else:
            response.add_error(message)
            response.set_status(status)

        return response

    def refresh_glue_table(self, crawler_name: str):
        try:
            result = self.client.start_crawler(Name=crawler_name)
        except ClientError as e:
            result = e.response
        return result

    def parse_response(self, glue_response: dict):
        error = glue_response.get('Error', {}).get('Code', '')
        status = glue_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = glue_response.get('Error', {}).get('Message')
        return error, status, message

    def parse_table_list_data(self, glue_response: dict):
        return list(map(lambda x: self.parse_table_data(x), glue_response['TableList']))

    def parse_table_data(self, glue_response: dict):
        table_parsed = {
            "name": glue_response.get("Name"),
            "created_at": glue_response.get("CreateTime"),
            "created_by": glue_response.get("CreatedBy"),
            "database_name": glue_response.get("DatabaseName"),
            "schema": glue_response.get("StorageDescriptor", {}).get("Columns"),

        }
        return table_parsed



