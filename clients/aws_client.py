from typing import Union

from engines.metastore.models.credentials import InvalidCredentials
from engines.metastore.models.credentials.aws import AWSCredentials

class AWSClient:

    def __init__(self, access_key: str, secret_key: str, aws_region: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.aws_region = aws_region

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        if self.access_key and self.secret_key:
            return AWSCredentials(self.access_key, self.secret_key)
        else:
            return InvalidCredentials("AWS Credentials Undefined.")
