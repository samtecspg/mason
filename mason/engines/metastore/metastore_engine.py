import importlib
from typing import Union, List, Optional

from mason.clients.base import Client
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.engines.engine import Engine
from mason.util.string import to_class_case
from mason.util.exception import message

class MetastoreEngine(Engine):
    
    def get_clients(self, metastore_clients: Optional[List[str]], all_clients: List[Client], client_module: str) -> List[Union[MetastoreClient, InvalidClient]]:
        if metastore_clients:
            return list(map(lambda c: self.get_client(c, all_clients, client_module), metastore_clients))
        else:
            return []

    def get_client(self, name: str, clients: List[Client], client_module: str) -> Union[MetastoreClient, InvalidClient]:
        cl = [c for c in clients if c.name() == name]
        if len(cl) > 0:
            client = cl[0]
            try:
                mod = importlib.import_module(f'{client_module}.{name}.metastore')
                class_name = to_class_case(name + "_metastore_client")
                ms_client = getattr(mod, class_name)(client)
                if isinstance(ms_client, MetastoreClient):
                    return ms_client
                else:
                    return InvalidClient("Could not get metastore cliet")
            except Exception as e:
                return InvalidClient(f"Could not instantiate metastore client for: {name}: {message(e)}")
        else:
            return InvalidClient(f"Client not configured: {name}")
