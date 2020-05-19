from typing import Tuple, Optional

from botocore.exceptions import ClientError

from engines.execution.models.jobs import Job
from engines.execution.models.jobs.infer_job import InferJob
from engines.execution.models.jobs.query_job import QueryJob
from util.uuid import uuid4

import boto3
from clients.response import Response

import awswrangler as wr
import pandas as pd

class AthenaClient:

    def __init__(self, config: dict):
        self.aws_region = config.get("aws_region")
        self.access_key = config.get("access_key") or ""
        self.secret_key = config.get("secret_key") or ""

    def client(self):
        return boto3.client('athena', region_name=self.aws_region, aws_secret_access_key=self.secret_key, aws_access_key_id=self.access_key)

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

    def query(self, job: QueryJob, response: Response) -> Tuple[Response, Job]:
        params = job.parameters
        response.add_info(f"Running job {job.type}")
        try:
            request_token = str(uuid4())
            athena_response = self.client().start_query_execution(
                QueryString=params["query_string"],
                ClientRequestToken=request_token,
                QueryExecutionContext={
                    'Database': params["database_name"]
                },
                WorkGroup='mason'
            )

        except ClientError as e:
            athena_response = e.response

        response.add_response(athena_response)
        error, status, message = self.parse_response(athena_response)

        if error == "AccessDeniedException":
            response.set_status(403)
            response.add_error(
                "Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena")
        elif not ((error or "") == ""):
            response.set_status(status)
            response.add_error(message)
        else:
            response.set_status(status)
            id = athena_response.get("QueryExecutionId")
            if id:
                job.id = id
                response = job.running(response)
        return response, job

    def infer(self, job: Job) -> Tuple[Response, Job]:
        


    def run_job(self, job: Job, response: Response) -> Tuple[Response, Optional[Job]]:
        if isinstance(job, QueryJob):
            response, job = self.query(job, response)
        else:
            response.add_error(f"Job type {job.type} not supported for Athena client")
        return response, job

