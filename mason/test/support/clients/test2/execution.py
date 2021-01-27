from mason.clients.engines.metastore import MetastoreClient
from mason.test.support.clients.test2 import Test2Client

class Test2ExecutionClient(MetastoreClient):

    def __init__(self, client: Test2Client):
        self.client = client
