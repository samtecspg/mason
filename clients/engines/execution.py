from clients import Client
from clients.response import Response
from abc import abstractmethod

class ExecutionClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific execution client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'SparkExecutionClient' with abstract attribute 'run_job' (for example)

    @abstractmethod
    def run_job(self, job_type: str, parameters: dict, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

class EmptyExecutionClient(ExecutionClient):

    def run_job(self, job_type: str, parameters: dict, response: Response) -> Response:
        # raise NotImplementedError("Client not implemented")
        return response
