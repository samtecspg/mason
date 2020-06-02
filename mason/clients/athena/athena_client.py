import boto3
from typing import Tuple, Union
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

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        # Parlaying over to glue for now
        glue_client = GlueClient({"access_key": self.access_key, "secret_key": self.secret_key, "aws_region": self.aws_region, "aws_role_arn": ""})
        return glue_client.get_database(database_name)

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

    def get_job(self, job_id: str, response: Response) -> Union[ExecutedJob, InvalidJob]:
        job = Job("query", response=response)
        job.set_id(job_id)

        try:
            athena_response = self.client().get_query_execution(
                QueryExecutionId=job_id,
            )
        except ClientError as e:
            athena_response = e.response


        reason, state, status, error, message = self.parse_execution_response(athena_response)

        job.response.add_response(athena_response)
        job.response.set_status(status)

        if reason:
            job.response.add_info(reason)

        if status == 200:
            job.response.add_info(f"Job Status: {state}")
            try:
                athena_response_2 = self.client().get_query_results(
                    QueryExecutionId=job_id,
                    MaxResults=10
                )
            except ClientError as e:
                athena_response_2 = e.response

            job.response.add_response(athena_response_2)

            error, status, message = self.parse_response(athena_response_2)


            if not ((error or "") == ""):
                job.response.set_status(status)
                return InvalidJob(job, message)
            else:
                job.response.set_status(status)
                results = athena_response_2.get("ResultSet")
                if results:
                    rows = results.get("Rows") or []
                    if len(rows) > 0:
                        job.response.add_data(results)
                    return ExecutedJob(job)
                else:
                    return InvalidJob(job, "No job results returned from athena")

        else:
            job.response.add_error(message)
            return InvalidJob(job, f"Invalid Job: {message}")

    def query(self, job: QueryJob) -> Union[ExecutedJob, InvalidJob]:
        job.add_log(f"Running Query \"{job.query_string}\"")

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

        job.response.add_response(athena_response)
        job.set_id(job.type + "_" + str(uuid4()))

        error, status, message = self.parse_response(athena_response)

        if error == "AccessDeniedException":
            job.response.set_status(403)
            return job.errored("Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena")
        elif not ((error or "") == ""):
            job.response.set_status(status)
            return job.errored(message)
        else:
            job.response.set_status(status)
            id = athena_response.get("QueryExecutionId")
            if id:
                job.set_id(id)
                return job.running(f"Running Athena query.  query_id: {id}")
            else:
                return job.errored("Query id not returned from athena")


    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        if isinstance(job, QueryJob):
            return self.query(job)
        else:
            return job.errored(f"Job type {job.type} not supported for Athena client")

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        statement = generate_ddl(df=table.as_df(), name=table.name, location=path.path_str, schema=database.name)
        return DDLStatement(statement)

    def execute_ddl(self, ddl: DDLStatement, database: Database) -> Union[ExecutedJob, InvalidJob]:
        ddl.statement
        job = QueryJob(ddl.statement, database)
        return self.query(job)


