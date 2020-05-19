from typing import Optional

from engines.metastore.models.credentials import MetastoreCredentials

class AWSCredentials(MetastoreCredentials):
    def __init__(self, access_key: Optional[str], secret_key: Optional[str]):
        self.access_key =  access_key
        self.secret_key = secret_key

    def to_dict(self):
        {'access_key': self.access_key, 'secret_key': self.secret_key }

