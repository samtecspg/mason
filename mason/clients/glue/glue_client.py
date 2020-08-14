from typing import Tuple, Union, List, Optional

import boto3
from botocore.client import BaseClient
from botocore.errorfactory import ClientError
from mason.engines.scheduler.models.schedule import Schedule

from mason.util.environment import MasonEnvironment

from mason.clients.aws_client import AWSClient
from mason.clients.response import Response
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.schemas.schema import SchemaElement, InvalidSchemaElement, Schema
from mason.engines.metastore.models.table import Table, InvalidTable, InvalidTables, TableNotFound
from mason.engines.storage.models.path import Path
from mason.util.list import sequence


class GlueClient(AWSClient):

    def __init__(self, config: dict):
        self.aws_role_arn = config.get("aws_role_arn")
        super().__init__(**config)

    def client(self) -> BaseClient:
        return boto3.client('glue', region_name=self.aws_region, aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Union[Database, InvalidDatabase], Response]:
        resp = response or Response()

        try:
            result = self.client().get_tables(DatabaseName=database_name)
        except ClientError as e:
            result = e.response

        error, status, message = self.parse_response(result)
        resp.add_response(result)

        if error == "EntityNotFoundException":
            resp.set_status(404)
            return InvalidDatabase(f"Database {database_name} not found"), resp
        elif 200 <= status < 300:

            table_list = result.get("TableList")
            if table_list:
                valid, invalid = sequence(list(map(lambda x: self.parse_table(x), table_list)), Table, InvalidTable)
                if len(invalid) > 0:
                    invalid_messages = ", ".join(list(map(lambda i: i.reason, invalid)))
                    resp.add_warning(f"Invalid Tables in glue response: {invalid_messages}")
                if len(valid) == 0:
                    return InvalidDatabase(f"No valid tables"), resp
                else:
                    return Database(database_name, valid), resp
            else:
                return InvalidDatabase("TableList not found in glue response"), resp
        else:
            resp.set_status(status)
            return InvalidDatabase(f"Invalid response from glue: {message}.  Status: {status}"), resp

    def list_tables(self, database_name: str, response: Response) -> Response:
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
            valid: List[Table]
            valid, invalid = self.parse_table_list_data(result)
            if len(valid) > 0:
                #  TODO move out
                data = {'Tables': list(map(lambda v: v.to_dict(), valid))}
                response.add_data(data)
            response.set_status(status)
        else:
            response.set_status(status)
            response.add_error(message)

        return response

    def delete_table(self, database_name: str, table_name: str, resp: Optional[Response] = None) -> Response:
        response = resp or Response()
        
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

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, schedule: Optional[Schedule], response: Response):
        create_crawler_response = self.create_glue_crawler(
            database=database_name,
            name=schedule_name,
            role=self.aws_role_arn or "",
            path=path.clean_path_str(),
            schedule=schedule
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
        table, response = self.get_table(database_name, table_name)

        crawler_name = None
        if isinstance(table, Table):
            created_by = table.created_by
            cb = created_by or ""
            if "crawler:" in cb:
                crawler_name = cb.replace("crawler:", "")
                self.trigger_schedule(crawler_name, response)
            else:
                response.add_error(f"Table not created by crawler. created_by: {created_by}")
        else:
            response.add_error(f"Could not find table {table_name}")
            response.set_status(404)

        return response

    def get_table(self, database_name: str, table_name: str,  resp: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        try:
            result = self.client().get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            result = e.response

        response: Response = resp or Response()
        response.add_response(result)

        error, status, message = self.parse_response(result)
        table = self.parse_table(result.get("Table", {}))

        final: Union[Table, InvalidTables]
        if error == "EntityNotFoundException":
            final = InvalidTables([TableNotFound(f"Database {database_name} or table {table_name} not found")])
        elif 200 <= status < 300:
            if isinstance(table, Table):
                final = table
            else:
                final = InvalidTables([table])
        else:
            final = InvalidTables([InvalidTable(f"Invalid Table: {message}")])
            response.set_status(status)
            
        return final, response

    def refresh_glue_table(self, crawler_name: str):
        try:
            result = self.client().start_crawler(Name=crawler_name)
        except ClientError as e:
            result = e.response
        return result

    def create_glue_crawler(self, database: str, name: str, role: str, path: str, schedule: Optional[Schedule]):
        targets = {
            'S3Targets': [
                {
                    'Path': path,
                    'Exclusions': []
                }
            ]
        }

        try:
            if schedule:
                result = self.client().create_crawler(
                    DatabaseName=database,
                    Name=name,
                    Role=role,
                    Targets=targets,
                    Schedule=schedule.definition
                )
            else:
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

    def parse_table(self, glue_response: dict) -> Union[Table, InvalidTable]:
        sd = glue_response.get("StorageDescriptor")
        crawler_name = glue_response.get("Parameters", {}).get("UPDATED_BY_CRAWLER")

        if sd:
            columns = sd.get("Columns")
            if columns:
                elements = list(map(lambda c: self.schema_from_dict(c), columns))
                valid, invalid = sequence(elements, SchemaElement, InvalidSchemaElement)
                if len(invalid) > 0:
                    invalid_messages = ", ".join(list(map(lambda i: i.reason, invalid)))
                    return InvalidTable(f"Invalid Schema, invalid elements: {invalid_messages}")
                elif len(valid) == 0:
                    return InvalidTable(f"Invalid Schema, no valid elements.")
                else:
                    name = glue_response.get("Name")
                    if name:
                        created_at = glue_response.get("CreateTime")

                        if crawler_name:
                            created_by = "crawler:" + crawler_name
                        else:
                            created_by = glue_response.get("CreatedBy")

                        schema = Schema(valid, "glue")
                        return Table(name, schema, created_at, created_by)
                    else:
                        return InvalidTable("No table Name found in glue response")

            else:
                return InvalidTable(f"Columns not found in glue response for {glue_response.get('Name', '')}")
        else:
            return InvalidTable("StorageDescriptor not found in glue response")

    def schema_from_dict(self, d: dict) -> Union[SchemaElement, InvalidSchemaElement]:
        name = d.get("Name")
        type = d.get("Type")
        if name and type:
            return SchemaElement(name, type)
        else:
            return InvalidSchemaElement("Name or Type for not found in SchemaElement")

    def parse_table_list_data(self, glue_response: dict) -> Tuple[List[Table], List[InvalidTable]]:
        table_list = glue_response.get('TableList')
        if isinstance(table_list, List):
            parsed: List[Union[Table, InvalidTable]] = list(map(lambda x: self.parse_table(x), table_list))
            valid, invalid = sequence(parsed, Table, InvalidTable)
            return valid, invalid
        else:
            none1: List[Table] = []
            none2: List[InvalidTable] = [InvalidTable("Bad TableList response from glue.  Expected list[dict]")]
            return none1, none2
