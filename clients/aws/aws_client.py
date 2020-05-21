from typing import Union

from engines.metastore.models.credentials import InvalidCredentials
from engines.metastore.models.credentials.aws import AWSCredentials

class AWSClient:

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        if self.access_key and self.secret_key:
            return AWSCredentials(self.access_key, self.secret_key)
        else:
            return InvalidCredentials("AWS Credentials Undefined.")
