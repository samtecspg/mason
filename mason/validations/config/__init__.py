from typing import List, Union, Type

from typistry.protos.invalid_object import InvalidObject
from typistry.protos.typed_dict import TypedDict
from typistry.protos.valid_dict import ValidDict
from typistry.validators.base import build_object, validate_dict
from typistry.protos.proto_object import ProtoObject

from mason.clients.base import Client
from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.engines.storage import StorageClient
from mason.configurations.config import Config
from mason.definitions import from_root
from mason.engines.execution.execution_engine import ExecutionEngine
from mason.engines.metastore.metastore_engine import MetastoreEngine
from mason.engines.scheduler.scheduler_engine import SchedulerEngine
from mason.engines.storage.storage_engine import StorageEngine

class ConfigProto(ProtoObject):
    
    # TODO: Make this dynamic like engines
    def supported_client(self, client_name: str) -> Type[Client]:
        if client_name == "spark":
            from mason.clients.spark.spark_client import SparkClient
            return SparkClient
        elif client_name == "s3":
            from mason.clients.s3.s3_client import S3Client
            return S3Client
        elif client_name == "glue":
            from mason.clients.glue.glue_client import GlueClient
            return GlueClient
        elif client_name == "dask":
            from mason.clients.dask.dask_client import DaskClient
            return DaskClient
        elif client_name == "athena":
            from mason.clients.athena.athena_client import AthenaClient
            return AthenaClient
        elif client_name == "local":
            from mason.clients.local.local_client import LocalClient
            return LocalClient
        else:
            return None

    def client_path(self) -> str:
        return "/clients/"

    def client_module(self) -> str:
        return "mason.clients"

    def build_class(self):
        return Config

    def build(self):

        attributes = self.dict.attributes()
        id = attributes.get("id")
        mc = attributes.get("metastore_clients")
        sc = attributes.get("storage_clients")
        cc = attributes.get("scheduler_clients")
        ec = attributes.get("execution_clients")
        source_path = attributes.get("source")

        cl = attributes.get("clients")

        clients: List[Client] = []
        invalid: List[InvalidClient] = []
        
        if id:
            # TODO: Add nested object and union support to typistry
            # TODO: Clean this up
            if isinstance(cl, dict):
                for client_name, configuration in cl.items():
                    configuration = configuration.get("configuration")
                    if isinstance(configuration, dict):
                        client_class = self.supported_client(client_name)
                        if not client_class is None:
                            tdict = TypedDict(configuration, client_name)
                            valid: Union[ValidDict, InvalidObject] = validate_dict(tdict, from_root(self.client_path()), True)._inner_value
                            if isinstance(valid, ValidDict):
                                valid.typed_dict.type = valid.type() + "_client"
                                value: Union[Client, InvalidObject] = build_object(valid, to_class=client_class)._inner_value
                                if isinstance(value, InvalidObject):
                                    invalid.append(InvalidClient(f"Invalid Client: {value.message}, {value.reference}"))
                                else:
                                    clients.append(value)
                            else:
                                invalid.append(InvalidClient(f"{valid.message}"))
                        else:
                            invalid.append(InvalidClient(f"Client not supported: {client_name}"))
                    else:
                        invalid.append(InvalidClient(f"Bad Configuration. Must be dict: {configuration}"))
            else:
                invalid.append(InvalidClient("Bad client configuration"))

            metastore_clients: List[Union[MetastoreClient, InvalidClient]] = MetastoreEngine().get_clients(mc, clients, self.client_module())
            execution_clients: List[Union[ExecutionClient, InvalidClient]] = ExecutionEngine().get_clients(ec, clients, self.client_module())
            scheduler_clients: List[Union[SchedulerClient, InvalidClient]] = SchedulerEngine().get_clients(cc, clients, self.client_module())
            storage_clients: List[Union[StorageClient, InvalidClient]] = StorageEngine().get_clients(sc, clients, self.client_module())
            
            return Config(id, clients, invalid, metastore_clients, execution_clients, storage_clients, scheduler_clients, source_path)
        else:
            return InvalidObject("Id not provided for config object", attributes)


