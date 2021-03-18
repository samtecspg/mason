from mason.clients.engines.execution import ExecutionClient
from mason.test.support.clients.test2 import Test2Client

class Test2ExecutionClient(ExecutionClient):

    def __init__(self, client: Test2Client):
        self.client = client
