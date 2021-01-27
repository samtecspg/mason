import importlib
from typing import Union, List, Optional

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.engines.engine import Engine
from mason.util.string import to_class_case
from mason.util.exception import message

class ExecutionEngine(Engine):
    
    def get_clients(self, execution_clients: Optional[List[str]], all_clients: List[Client], client_module: str) -> List[Union[ExecutionClient, InvalidClient]]:
        if execution_clients:
            return list(map(lambda c: self.get_client(c, all_clients, client_module), execution_clients))
        else:
            return []

    def get_client(self, name: str, clients: List[Client], client_module: str) -> Union[ExecutionClient, InvalidClient]:
        cl = [c for c in clients if c.name() == name]
        if len(cl) > 0:
            client = cl[0]
            try:
                mod = importlib.import_module(f'{client_module}.{name}.execution')
                class_name = to_class_case(name + "_execution_client")
                ms_client = getattr(mod, class_name)(client)
                return ms_client
            except Exception as e:
                return InvalidClient(f"Could not instantiate execution client for: {name}: {message(e)}")
        else:
            return InvalidClient(f"Client not configured: {name}")

    # def set_mode(self, mode: str):
    #     if mode.lower() == "async":
    #         self.mode = "async"
    #     elif mode.lower() == "sync":
    #         self.mode = "sync"
    #     else:
    #         logger.error(f"Invalid Mode: {mode}")
    #         pass
