from engines.metastore.models.credentials import MetastoreCredentials
from os import environ

class AWSCredentials(MetastoreCredentials):
    def __init__(self, access_key: str, secret_key: str):
        self.type = "aws"
        self.access_key = environ.get("AWS_ACCESS_KEY_ID") or access_key
        self.secret_key = environ.get("AWS_SECRET_ACCESS_KEY") or secret_key

