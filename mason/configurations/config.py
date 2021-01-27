from typing import List, Union

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.storage import StorageClient
from mason.configurations import REDACTED_KEYS
from mason.util.dict import sanitize


class Config():
    
    def __init__(self, id: str,
            clients: List[Client],
            invalid_clients: List[InvalidClient],
            metastore_clients: List[Union[MetastoreClient, InvalidClient]],
            execution_clients: List[Union[ExecutionClient, InvalidClient]],
            storage_clients: List[Union[StorageClient, InvalidClient]],
            scheduler_clients: List[Union[SchedulerClient, InvalidClient]]):
        self.id = id
        self.clients = clients
        self.invalid_clients = invalid_clients
        self.metastore_clients = metastore_clients
        self.execution_clients = execution_clients
        self.storage_clients = storage_clients
        self.scheduler_clients = scheduler_clients
        
    def metastore(self) -> Union[MetastoreClient, InvalidClient]:
        if len(self.metastore_clients) >= 1:
            return self.metastore_clients[0]
        elif len(self.metastore_clients) == 0:
            return InvalidClient("No metastores")
        else:
            raise Exception("Multiple metastore clients configured. Please specify metastore_client by name")
        
    def execution(self) -> Union[ExecutionClient, InvalidClient]:
        if len(self.execution_clients) >= 1:
            return self.execution_clients[0]
        elif len(self.execution_clients) == 0:
            return InvalidClient("No executions")
        else:
            raise Exception("Multiple execution clients configured. Please specify metastore_client by name")

    def scheduler(self) -> Union[SchedulerClient, InvalidClient]:
        if len(self.scheduler_clients) >= 1:
            return self.scheduler_clients[0]
        elif len(self.scheduler_clients) == 0:
            return InvalidClient("No schedulers")
        else:
            raise Exception("Multiple scheduler clients configured. Please specify metastore_client by name")
        
    def storage(self) -> Union[StorageClient, InvalidClient]:
        if len(self.storage_clients) >= 1:
            return self.storage_clients[0]
        elif len(self.storage_clients) == 0:
            return InvalidClient("Not storages")
        else:
            raise Exception("Multiple storage clients configured. Please specify metastore_client by name")

    def extended_info(self, config_id: str, current: bool = False):
        return []
        # ei = []
        # 
        # if not self.metastore(). == "":
        #     ei.append(
        #         [
        #             "",
        #             "metastore",
        #             self.metastore.client_name,
        #             sanitize(self.metastore.config, REDACTED_KEYS),
        #         ]
        #     )
        # 
        # if not self.scheduler.client_name == "":
        #     ei.append(
        #         [
        #             "",
        #             "scheduler",
        #             self.scheduler.client_name,
        #             sanitize(self.scheduler.config, REDACTED_KEYS),
        #         ]
        #     )
        # 
        # if not self.storage.client_name == "":
        #     ei.append(
        #         [
        #             "",
        #             "storage",
        #             self.storage.client_name,
        #             sanitize(self.storage.config, REDACTED_KEYS),
        #         ]
        #     )
        # 
        # if not self.execution.client_name == "":
        #     ei.append(
        #         [
        #             "",
        #             "execution",
        #             self.execution.client_name,
        #             sanitize(self.execution.config, REDACTED_KEYS),
        #         ]
        #     )
        # 
        # def with_id_item(l, current: bool):
        #     if current == True:
        #         cid = "*  " + str(config_id)
        #     else:
        #         cid = ".  " + str(config_id)
        #     return [cid if l.index(x) == 0 else x for x in l]
        # 
        # with_id = [with_id_item(x, current) if ei.index(x) == 0 else x for x in ei]
        # 
        # return with_id
