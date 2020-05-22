
from abc import abstractmethod
from typing import Union

from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob


class DaskRunner:

    @abstractmethod
    def run(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob("Runner not implemented")

class EmptyDaskRunner(DaskRunner):

    def run(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob("Runner not implemented")


