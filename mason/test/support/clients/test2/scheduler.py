from mason.clients.engines.scheduler import SchedulerClient
from mason.test.support.clients.test import TestClient

class Test2SchedulerClient(SchedulerClient):

    def __init__(self, client: TestClient):
        self.client = client
