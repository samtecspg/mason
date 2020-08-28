from typing import Tuple, Union, Optional

from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job

class KubernetesWorkerMock:
    
    def run(self, job: Job, resp: Optional[Response] = None, mode: str = "async") -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        r = resp or Response()
        return (ExecutedJob("HERE"), r)

