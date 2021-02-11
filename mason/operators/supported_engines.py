
from typing import Optional, List, Dict, Union

from mason.clients.base import Client
from mason.clients.engines.invalid_client import InvalidClient
from mason.configurations.config import Config

def from_array(a: List[Dict[str, str]]):
    return list(map(lambda item: SupportedEngineSet(metastore=item.get("metastore"), scheduler=item.get("scheduler"), execution=item.get("execution"), storage=item.get("storage")), a))

class SupportedEngineSet:

    def __init__(self, metastore: Optional[str], scheduler: Optional[str], execution: Optional[str], storage: Optional[str]):
        self.metastore = metastore
        self.scheduler = scheduler
        self.execution = execution
        self.storage = storage

        self.all = {
            'metastore': self.metastore,
            'scheduler': self.scheduler,
            'execution': self.execution,
            'storage': self.storage
        }

    def validate_coverage(self, config: Config):
        for engine_type, supported_engine in self.all.items():
            clients: List[Union[Client, InvalidClient]] = config.__getattribute__(f"{engine_type}_clients")
            client_names = list(map(lambda c: c.client.name(), clients))
            if supported_engine in client_names:
                return True
            else:
                return False


