import boto3
from typing import Tuple, Union, Optional
from botocore.exceptions import ClientError
from pyathena.util import generate_ddl

from mason.clients.glue.glue_client import GlueClient
from mason.clients.aws_client import AWSClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table
from mason.engines.storage.models.path import Path
from mason.util.uuid import uuid4


class AthenaClient(AWSClient):

    def __init__(self, config: dict):
        super().__init__(**config)

    def client(self):
        return boto3.client('athena', region_name=self.aws_region, aws_secret_access_key=self.secret_key, aws_access_key_id=self.access_key)

    def get_database(self, database_name: str, response: Optional[Response]) -> Tuple[Union[Database, InvalidDatabase], Response]:
        # Parlaying over to glue for now
        glue_client = GlueClient({"access_key": self.access_key, "secret_key": self.secret_key, "aws_region": self.aws_region, "aws_role_arn": ""})
        return glue_client.get_database(database_name, response)

    def parse_execution_response(self, athena_response: dict) -> Tuple[str, str, int, str, str]:
        reason = athena_response.get("QueryExecution", {}).get("Status", {}).get("StateChangeReason")
        state = athena_response.get("QueryExecution", {}).get("Status", {}).get("State", "")
        status = athena_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        error = athena_response.get('Error', {}).get('Code', '')
        message = athena_response.get('Error', {}).get('Message')

        return reason, state, status, error, message

    def parse_response(self, athena_response: dict):
        error = athena_response.get('Error', {}).get('Code', '')
        status = athena_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = athena_response.get('Error', {}).get('Message')
        return error, status, message

    def get_job(self, job_id: str, resp: Optional[Response]) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        job = Job("query")
        job.set_id(job_id)
        
        response: Response = resp or Response()

        try:
            athena_response = self.client().get_query_execution(
                QueryExecutionId=job_id,
            )
        except ClientError as e:
            athena_response = e.response


        reason, state, status, error, message = self.parse_execution_response(athena_response)

        response.add_response(athena_response)
        response.set_status(status)

        if reason:
            response.add_info(reason)

        final: Union[ExecutedJob, InvalidJob]
        if status == 200:
            response.add_info(f"Job Status: {state}")
            try:
                athena_response_2 = self.client().get_query_results(
                    QueryExecutionId=job_id,
                    MaxResults=10
                )
            except ClientError as e:
                athena_response_2 = e.response

            response.add_response(athena_response_2)

            error, status, message = self.parse_response(athena_response_2)


            if not ((error or "") == ""):
                response.set_status(status)
                final =  job.errored(message)
            else:
                response.set_status(status)
                results = athena_response_2.get("ResultSet")
                if results:
                    rows = results.get("Rows") or []
                    if len(rows) > 0:
                        response.add_data(results)
                    final = job.running(past=True) 
                else:
                    final =  job.errored("No job results returned from athena")

        else:
            response.add_error(message)
            final =  job.errored(f"Invalid Job: {message}")
            
        return final, response

    def query(self, job: QueryJob, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        
        response = resp or Response()

        try:
            request_token = str(uuid4())
            athena_response = self.client().start_query_execution(
                QueryString=job.query_string,
                ClientRequestToken=request_token,
                QueryExecutionContext={
                    'Database': job.database.name
                },
                WorkGroup='mason'
            )

        except ClientError as e:
            athena_response = e.response

        response.add_response(athena_response)
        job.set_id(job.type + "_" + str(uuid4()))

        error, status, message = self.parse_response(athena_response)

        if error == "AccessDeniedException":
            response.set_status(403)
            return job.errored("Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena"), response
        elif not ((error or "") == ""):
            response.set_status(status)
            return job.errored(message), response
        else:
            response.set_status(status)
            id = athena_response.get("QueryExecutionId")
            if id:
                job.set_id(id)
                return job.running(f"Running Athena query.  query_id: {id}"), response
            else:
                return job.errored("Query id not returned from athena"), response


    def run_job(self, job: Job, resp: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        response: Response = resp or Response()
        
        final: Union[ExecutedJob, InvalidJob]
        if isinstance(job, QueryJob):
            final, response =  self.query(job, resp)
        else:
            final =  job.errored(f"Job type {job.type} not supported for Athena client")
            
        return final, response

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        statement = generate_ddl(df=table.as_df(), name=table.name, location=path.path_str, schema=database.name)
        return DDLStatement(statement)

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        job = QueryJob(ddl.statement, database)
        return self.query(job, response)


