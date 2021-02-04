from typing import List, Union, Optional

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.storage import StorageClient
from mason.configurations import REDACTED_KEYS
from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore
from mason.util.dict import sanitize


class Config(Saveable):
    
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
        
    def metastore(self) -> MetastoreClient:
        if len(self.metastore_clients) >= 1:
            return self.metastore_clients[0]
        elif len(self.metastore_clients) == 0:
            raise Exception("No metastores")
        else:
            raise Exception("Multiple metastore clients configured. Please specify metastore_client by name")
        
    def execution(self) -> ExecutionClient:
        if len(self.execution_clients) >= 1:
            return self.execution_clients[0]
        elif len(self.execution_clients) == 0:
            raise Exception("No executions")
        else:
            raise Exception("Multiple execution clients configured. Please specify metastore_client by name")

    def scheduler(self) -> SchedulerClient:
        if len(self.scheduler_clients) >= 1:
            return self.scheduler_clients[0]
        elif len(self.scheduler_clients) == 0:
            raise Exception("No schedulers")
        else:
            raise Exception("Multiple scheduler clients configured. Please specify metastore_client by name")
        
    def storage(self) -> StorageClient:
        if len(self.storage_clients) >= 1:
            return self.storage_clients[0]
        elif len(self.storage_clients) == 0:
            raise Exception("Not storages")
        else:
            raise Exception("Multiple storage clients configured. Please specify metastore_client by name")

    def save(self, state_store: MasonStateStore, overwrite: bool = False):
        state_store.cp_source(self.source_path, "config", overwrite=overwrite)

    def extended_info(self, current_id: Optional[str] = None) -> List[str]:
        execution_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.execution_clients)))
        storage_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.storage_clients)))
        scheduler_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.scheduler_clients)))
        metastore_clients: str = ", ".join(list(map(lambda c: c.client.name(), self.metastore_clients)))
        return [self.id, execution_clients, metastore_clients, storage_clients, scheduler_clients]


