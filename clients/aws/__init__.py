from engines.metastore.models.credentials.aws import AWSCredentials

from clients.response import Response


class AWSClient:

    def credentials(self, response: Response):
        if self.access_key and self.secret_key:
            return AWSCredentials(self.access_key, self.secret_key), response
        else:
            response.add_error("AWS Credentials Undefined")
