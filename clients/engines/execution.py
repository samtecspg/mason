from clients import Client, EmptyClient
from clients.response import Response
from abc import abstractmethod
from engines.metastore.models.credentials import MetastoreCredentials

class ExecutionClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific execution client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'SparkExecutionClient' with abstract attribute 'run_job' (for example)

    @abstractmethod
    def run_job(self, job_type: str, metastore_credentials: MetastoreCredentials, parameters: dict, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    @abstractmethod
    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

class EmptyExecutionClient(ExecutionClient):

    def run_job(self, job_type: str, metastore_credentials: MetastoreCredentials, parameters: dict, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response
