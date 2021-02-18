
from typing import Optional, List, Dict, Union, Tuple

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.storage import StorageClient
from mason.configurations.config import Config

def from_array(a: List[Dict[str, str]]):
    return list(map(lambda item: SupportedEngineSet(metastore=item.get("metastore"), scheduler=item.get("scheduler"), execution=item.get("execution"), storage=item.get("storage")), a))

class SupportedEngineSet:

    def __init__(self, metastore: Optional[str], scheduler: Optional[str], execution: Optional[str], storage: Optional[str]):
        self.metastore = metastore
        self.scheduler = scheduler
        self.execution = execution
        self.storage = storage
        
        all = {}
        if self.metastore:
            all['metastore'] = self.metastore
        if self.scheduler:
            all['scheduler'] = self.scheduler
        if self.execution:
            all['execution'] = self.execution
        if self.storage:
            all['storage'] = self.storage

        self.all = all

    def validate_coverage(self, config: Config) -> Tuple[bool, str]:
        test = True
        message = ""
        # TODO:  Make this support multiple supported engines
        for engine_type, supported_engine in self.all.items():
            clients: List[Union[MetastoreClient, SchedulerClient, StorageClient, ExecutionClient, InvalidClient]] = config.__getattribute__(f"{engine_type}_clients")
            client_names = list(map(lambda c: c.client.name(), clients))
            if supported_engine:
                if not supported_engine in client_names:
                    test = False
                    message = f"Clients {client_names} do not include supported client {supported_engine} for {engine_type}"
                    break
                    
        return test, message
            