from typing import Tuple

import boto3 # type: ignore
from clients.response import Response

from engines.metastore.models.credentials import MetastoreCredentials

class AthenaClient:

    def __init__(self, config: dict):
        self.client = boto3.client('athena')
        self.aws_role_arn = config.get("aws_role_arn", "")

    def run_job(self, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        response.add_info(f"Running job {job_name}")
        return response
