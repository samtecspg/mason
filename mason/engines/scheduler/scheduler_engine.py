import importlib
from typing import Union, List, Optional

from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.base import Client
from mason.engines.engine import Engine
from mason.util.string import to_class_case
from mason.util.exception import message

class SchedulerEngine(Engine):

    def get_clients(self, scheduler_clients: Optional[List[str]], all_clients: List[Client], client_module: str) -> List[Union[SchedulerClient, InvalidClient]]:
        if scheduler_clients:
            return list(map(lambda c: self.get_client(c, all_clients, client_module), scheduler_clients))
        else:
            return []

    def get_client(self, name: str, clients: List[Client], client_module: str) -> Union[SchedulerClient, InvalidClient]:
        result: Union[SchedulerClient, InvalidClient]
        cl = [c for c in clients if c.name() == name]
        if len(cl) > 0:
            client = cl[0]
            try:
                mod = importlib.import_module(f'{client_module}.{name}.scheduler')
                class_name = to_class_case(name + "_scheduler_client")
                ms_client = getattr(mod, class_name)(client)
                if isinstance(ms_client, SchedulerClient):
                    result= ms_client
                else:
                    result= InvalidClient(f"Is not scheduler client: {ms_client}")
            except Exception as e:
                result= InvalidClient(f"Could not instantiate scheduler client for: {name}: {message(e)}")
        else:
            result= InvalidClient(f"Client not configured: {name}")
            
        return result
