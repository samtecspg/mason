from mason.clients.engines.metastore import MetastoreClient
from mason.test.support.clients.test import TestClient

class TestMetastoreClient(MetastoreClient):

    def __init__(self, client: TestClient):
        self.client = client
