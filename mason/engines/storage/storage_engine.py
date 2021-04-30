import importlib
from typing import Union, List, Optional

from mason.clients.base import Client
from mason.clients.engines.storage import StorageClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.engines.engine import Engine
from mason.engines.storage.models.path import Path
from mason.util.string import to_class_case
from mason.util.exception import message

class StorageEngine(Engine):
    
    def get_clients(self, storage_clients: Optional[List[str]], all_clients: List[Client], client_module: str) -> List[Union[StorageClient, InvalidClient]]:
        if storage_clients:
            return list(map(lambda c: self.get_client(c, all_clients, client_module), storage_clients))
        else:
            return []

    def get_client(self, name: str, clients: List[Client], client_module: str) -> Union[StorageClient, InvalidClient]:
        cl = [c for c in clients if c.name() == name]
        if len(cl) > 0:
            client = cl[0]
            try:
                mod = importlib.import_module(f'{client_module}.{name}.storage')
                class_name = to_class_case(name + "_storage_client")
                storage_client = getattr(mod, class_name)(client)
                if isinstance(storage_client, StorageClient):
                    return storage_client
                else:
                    return InvalidClient("Could not fetch storage client")
            except Exception as e:
                return InvalidClient(f"Could not instantiate storage client for: {name}: {message(e)}")
        else:
            return InvalidClient(f"Client not configured: {name}")
    
    
