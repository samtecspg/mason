from typing import Union, Optional

from mason.clients.engines.scheduler import SchedulerClient
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.test.support.clients.test import TestClient

class Test2SchedulerClient(SchedulerClient):

    def __init__(self, client: TestClient):
        self.client = client

    def validate_schedule(self, schedule: Optional[str] = None) -> Union[Optional[Schedule], InvalidSchedule]:
        return None
