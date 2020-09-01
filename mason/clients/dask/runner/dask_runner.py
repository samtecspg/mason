
from abc import abstractmethod
from typing import Union, Optional, Tuple

from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

class DaskRunner:

    @abstractmethod
    def run(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Runner not implemented")

class EmptyDaskRunner(DaskRunner):

    def run(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Empty Runner: not implemented")


