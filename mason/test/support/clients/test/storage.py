from mason.clients.engines.storage import StorageClient
from mason.test.support.clients.test import TestClient

class TestStorageClient(StorageClient):

    def __init__(self, client: TestClient):
        self.client = client
