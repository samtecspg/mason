from typing import Union, Optional

from mason.clients.base import Client
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials

class AWSClient(Client):

    def __init__(self, access_key: str, secret_key: str, aws_region: str, aws_role_arn: Optional[str] = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.aws_region = aws_region
        self.aws_role_arn = aws_role_arn

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        if self.access_key and self.secret_key:
            return AWSCredentials(self.access_key, self.secret_key)
        else:
            return InvalidCredentials("AWS Credentials Undefined.")

