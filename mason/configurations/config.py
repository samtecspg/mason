from typing import List  

from mason.clients.base import Client
from mason.clients.engines.invalid_client import InvalidClient

class Config():
    
    def __init__(self, id: str, clients: List[Client], invalid_clients: List[InvalidClient]):
        self.id = id
        self.clients = clients
        self.invalid_clients = invalid_clients
        
    # def config_schema(self) -> str:
    #     return from_root("/configurations/schema.json")
    # 
    # def validate(self, source_path: Optional[str] = None) -> Union[ValidConfig, InvalidConfig]:
    #     config_schema = self.config_schema()
    #     schema = validate_schema(self.config, config_schema)
    # 
    #     valid_config: Optional[ValidConfig]
    #     invalid_config: Optional[InvalidConfig]
    #     id = str(self.id)
    #     if id:
    #         if isinstance(schema, ValidSchemaDict):
    #             me = MetastoreEngine(schema.dict)
    #             ce = SchedulerEngine(schema.dict)
    #             se = StorageEngine(schema.dict)
    #             ee = ExecutionEngine(schema.dict)
    # 
    #             if isinstance(me.client, InvalidClient) or isinstance(ce.client, InvalidClient) or isinstance(se.client, InvalidClient) or isinstance(ee.client, InvalidClient):
    #                 reason = "Invalid Engine Configuration. "
    #                 for e in [me.client, ce.client, se.client, ee.client]:
    #                     if isinstance(e, InvalidClient):
    #                         reason += e.reason + ". "
    # 
    #                 return InvalidConfig(self.config, reason)
    #             else:
    #                 return ValidConfig(id, self.config, me, ce, se, ee, source_path)
    # 
    #         else:
    #             return InvalidConfig(self.config, f"Invalid config schema. Reason: {schema.reason}")
    # 
    #     else:
    #         return InvalidConfig(self.config, f"Config id not specified as string: {id}")
    # 
