from configurations.response import Response
import boto3 # type: ignore
import s3fs # type: ignore
from botocore.errorfactory import ClientError # type: ignore
from typing import Optional

class GlueSchedulerClient:
    # constructor
    def __init__(self, aws_role_arn: Optional[str]):
        self.aws_role_arn = aws_role_arn
        self.client = boto3.client('glue', region_name=self.region)

    # def refresh_table(self, name: str, database_name: str, response: Response):
    #     # if crawler_name == None:
    #     crawler_name = None
    #     get_glue_table_response = self.get_table(database_name, name, response)
    #     if 200 <= get_glue_table_response.status_code < 300:
    #         crawler_name = get_glue_table_response.responses[-1].get("Table", {}).get("Parameters", {}).get("UPDATED_BY_CRAWLER")
    #
    #     if crawler_name:
    #         refresh_glue_table_response = self.refresh_glue_table(crawler_name)
    #         error, status, message = self.parse_response(refresh_glue_table_response)
    #
    #         response.add_response(refresh_glue_table_response)
    #
    #         if error == "CrawlerRunningException":
    #             response.add_warning(f"Table crawler {crawler_name} is already refreshing.")
    #             response.set_status(202)
    #         elif status:
    #             if 200 <= status < 300:
    #                 response.add_info(f"Refreshing Table Crawler: {crawler_name}")
    #                 response.set_status(201)
    #         else:
    #             response.add_error(message)
    #             response.set_status(status)
    #     # else:
    #     #     response.add_error("Could not find crawler for table")
    #     #     response.set_status(404)
    #
    #     return response

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



