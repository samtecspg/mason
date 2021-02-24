from typing import List, Union, Optional

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.resources.resource import Resource
from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore, FailedOperation
# from mason.configurations import REDACTED_KEYS
# from mason.util.dict import sanitize
from mason.util.exception import message

class Config(Saveable, Resource):
    
    def __init__(self, id: str,
            clients: List[Client],
            invalid_clients: List[InvalidClient],
            metastore_clients: List[Union[MetastoreClient, InvalidClient]],
            execution_clients: List[Union[ExecutionClient, InvalidClient]],
            storage_clients: List[Union[StorageClient, InvalidClient]],
            scheduler_clients: List[Union[SchedulerClient, InvalidClient]],
            source_path: Optional[str] = None):
        
        super().__init__(source_path)
        self.id = id
        self.clients = clients
        self.invalid_clients = invalid_clients
        self.metastore_clients = metastore_clients
        self.execution_clients = execution_clients
        self.storage_clients = storage_clients
        self.scheduler_clients = scheduler_clients
        self.source_path = source_path
        
    def metastore(self) -> Union[MetastoreClient, InvalidClient]:
        if len(self.metastore_clients) >= 1:
            return self.metastore_clients[0]
        elif len(self.metastore_clients) == 0:
            return InvalidClient("No metastores")
        else:
            return InvalidClient("Multiple metastore clients configured. Please specify metastore_client by name")
        
    def execution(self) -> Union[ExecutionClient, InvalidClient]:
        if len(self.execution_clients) >= 1:
            return self.execution_clients[0]
        elif len(self.execution_clients) == 0:
            return InvalidClient("No executions")
        else:
            return InvalidClient("Multiple execution clients configured. Please specify execution_client by name")

    def scheduler(self) -> Union[SchedulerClient, InvalidClient]:
        if len(self.scheduler_clients) >= 1:
            return self.scheduler_clients[0]
        elif len(self.scheduler_clients) == 0:
            return InvalidClient("No schedulers")
        else:
            return InvalidClient("Multiple scheduler clients configured. Please specify scheduler_client by name")
        
    def storage(self) -> Union[StorageClient, InvalidClient]:
        if len(self.storage_clients) >= 1:
            return self.storage_clients[0]
        elif len(self.storage_clients) == 0:
            return InvalidClient("Not storages")
        else:
            return InvalidClient("Multiple storage clients configured. Please specify storage_client by name")

    def save(self, state_store: MasonStateStore, overwrite: bool = False, response: Response = Response()) -> Response:
        try:
            result = state_store.cp_source(self.source_path, "config", overwrite=overwrite)
            if isinstance(result, FailedOperation):
                response.add_error(f"{result.message}")
            else:
                response.add_info(result)
        except Exception as e:
            response.add_error(f"Error copying source: {message(e)}")
        return response

    def extended_info(self, current_id: Optional[str] = None) -> List[str]:
        id = self.id
        if current_id and current_id == id:
            id += " *"
        execution_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.execution_clients)))
        storage_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.storage_clients)))
        scheduler_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.scheduler_clients)))
        metastore_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.metastore_clients)))
        return [id, execution_clients, metastore_clients, storage_clients, scheduler_clients]
    
    def to_dict(self, current_id: Optional[str]) -> dict:
        execution_clients = list(map(lambda c: c.client.to_dict(), self.execution_clients))
        storage_clients = list(map(lambda c: c.client.to_dict(), self.storage_clients))
        scheduler_clients = list(map(lambda c: c.client.to_dict(), self.scheduler_clients))
        metastore_clients = list(map(lambda c: c.client.to_dict(), self.metastore_clients))
        
        id = self.id
        if current_id and current_id == id:
            current = True
        else:
            current = False
        return {'current': current, 'id': id, 'execution_client': execution_clients, 'metastore_client': metastore_clients, 'storage_client': storage_clients, 'scheduler_client': scheduler_clients}


