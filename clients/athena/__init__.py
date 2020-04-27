from typing import Tuple

from botocore.exceptions import ClientError # type: ignore

from engines.execution.models.jobs import Job
from util.uuid import uuid4

import boto3 # type: ignore
from clients.response import Response

from engines.metastore.models.credentials import MetastoreCredentials

class AthenaClient:

    def __init__(self, config: dict):
        self.access_key = config.get("access_key") or ""
        self.secret_key = config.get("secret_key") or ""

    def client(self):
        return boto3.client('athena', aws_secret_access_key=self.secret_key, aws_access_key_id=self.access_key)

    def parse_response(self, athena_response: dict):
        error = athena_response.get('Error', {}).get('Code', '')
        status = athena_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = athena_response.get('Error', {}).get('Message')
        return error, status, message

    def get_job(self, job_id: str, response: Response) -> Tuple[Job, Response]:
        try:
            athena_response = self.client().get_query_results(
                QueryExecutionId=job_id,
                MaxResults=10
            )
        except ClientError as e:
            athena_response = e.response

        response.add_response(athena_response)
        error, status, message = self.parse_response(athena_response)

        if not ((error or "") == ""):
            response.set_status(status)
            job = Job(job_id, errors=[message])
        else:
            response.set_status(status)
            results = athena_response.get("ResultSet")
            job = Job(job_id, results=[results])
        return response, job

    def run_job(self, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response) -> Tuple[Response, Job]:
        response.add_info(f"Running job {job_name}")
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
            response.set_status(status)
            response.add_error("Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena")
        elif not ((error or "") == ""):
            response.set_status(status)
            response.add_error(message)
        else:
            response.set_status(status)
            id = athena_response.get("QueryExecutionId")
            if id:
                job = Job(id)
                response = job.running(response)

        return response, job

