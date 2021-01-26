from typing import List, Union

from typistry.protos.invalid_object import InvalidObject
from typistry.protos.typed_dict import TypedDict
from typistry.protos.valid_dict import ValidDict
from typistry.validators.base import build_object, validate_dict

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.base import Client
from mason.clients.dask.dask_client import DaskClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.local.local_client import LocalClient
from mason.clients.s3.s3_client import S3Client
from mason.clients.spark.spark_client import SparkClient
from mason.configurations.config import Config
from typistry.protos.proto_object import ProtoObject
from mason.definitions import from_root

class ConfigProto(ProtoObject):
    
    def supported_clients(self) -> dict:
        return {
            "spark": SparkClient,
            "s3": S3Client,
            "glue": GlueClient,
            "dask": DaskClient,
            "athena": AthenaClient,
            "local": LocalClient
        }

    def client_path(self) -> str:
        return "/clients/"

    def build_class(self):
        return Config

    def build(self):

        attributes = self.dict.attributes()
        id = attributes.get("id")
        me = attributes.get("metastore_engines")
        se = attributes.get("storage_engines")
        ce = attributes.get("scheduler_engines")
        ee = attributes.get("execution_engines")

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
                        client_class = self.supported_clients().get(client_name)
                        if not client_class is None:
                            tdict = TypedDict(configuration, client_name)
                            valid: Union[ValidDict, InvalidObject] = validate_dict(tdict, from_root(
                                self.client_path()))._inner_value
                            if isinstance(valid, ValidDict):
                                valid.typed_dict.type = valid.type() + "_client"
                                clients.append(build_object(valid, to_class=client_class)._inner_value)
                            else:
                                invalid.append(InvalidClient(f"{valid.message}"))
                        else:
                            invalid.append(InvalidClient(f"Client not supported: {client_name}"))
                    else:
                        invalid.append(f"Bad Configuration. Must be dict: {configuration}")
            else:
                invalid.append("Bad client configuration")

            return Config(id, clients, invalid)
        else:
            return InvalidObject(attributes, "Id not provided for config object")
