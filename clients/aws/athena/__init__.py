from typing import Tuple, Optional, Union

from botocore.exceptions import ClientError

from clients.aws.glue import GlueClient
from engines.metastore.models.database import Database, InvalidDatabase
from engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from engines.metastore.models.table import Table
from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from engines.execution.models.jobs.query_job import QueryJob
from engines.storage.models.path import Path
from util.uuid import uuid4

import boto3
from clients.response import Response
from pyathena.util import generate_ddl

class AthenaClient:

    def __init__(self, config: dict):
        self.aws_region = config.get("aws_region")
        self.access_key = config.get("access_key") or ""
        self.secret_key = config.get("secret_key") or ""

    def client(self):
        return boto3.client('athena', region_name=self.aws_region, aws_secret_access_key=self.secret_key, aws_access_key_id=self.access_key)

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        # Parlaying over to glue for now
        glue_client = GlueClient({"access_key": self.access_key, "secret_key": self.secret_key, "region": self.aws_region})
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

    def get_job(self, job_id: str, response: Response) -> Tuple[Response, Job]:
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

        if status == 200:
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
                job = Job(job_id, errors=[message])
            else:
                response.set_status(status)
                results = athena_response_2.get("ResultSet")
                if results:
                    job = Job(job_id, results=[results])
                else:
                    job = Job(job_id)

        else:
            job = Job(job_id, errors=[message])
        return response, job

    def query(self, job: QueryJob) -> Union[ExecutedJob, InvalidJob]:
        job.add_log(f"Running job {job.type}")

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

        job.add_data(athena_response)
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
                return job.errored(InvalidJob("Query id not returned from athena"))


    def run_job(self, job: Job, response: Response) -> Union[ExecutedJob, InvalidJob]:
        if isinstance(job, QueryJob):
            return self.query(job, response)
        else:
            return job.errored(f"Job type {job.type} not supported for Athena client")

    def generate_table_ddl(self, table: Table, output_path: Optional[Path]) -> Union[DDLStatement, InvalidDDLStatement]:
        if output_path:
            statement = generate_ddl(table.as_df(), table.name, output_path.path_str)
            return DDLStatement(statement)
        else:
            return InvalidDDLStatement("Athena requires output path location to store parquet files")

    def execute_ddl(self, ddl: DDLStatement, database: Database) -> Union[ExecutedJob, InvalidJob]:
        ddl.statement
        job = QueryJob(ddl.statement, database)
        return self.query(job)

