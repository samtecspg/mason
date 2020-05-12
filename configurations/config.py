from typing import Optional, Union

from configurations.valid_config import ValidConfig
from configurations.invalid_config import InvalidConfig
from engines.execution import ExecutionEngine
from clients.engines.invalid_client import InvalidClient
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine
from util.environment import MasonEnvironment
from util.json_schema import validate_schema, ValidSchemaDict
from util.logger import logger

class Config:

    def __init__(self, config: Optional[dict]):
        self.config = config or {}

    def validate(self, env: MasonEnvironment) -> Union[ValidConfig, InvalidConfig]:
        schema = validate_schema(self.config, env.config_schema)
        valid_config: Optional[ValidConfig]
        invalid_config: Optional[InvalidConfig]

        if isinstance(schema, ValidSchemaDict):
            me = MetastoreEngine(schema.dict)
            ce = SchedulerEngine(schema.dict)
            se = StorageEngine(schema.dict)
            ee = ExecutionEngine(schema.dict)

            if isinstance(me.client, InvalidClient) or isinstance(ce.client, InvalidClient) or isinstance(se.client, InvalidClient) or isinstance(ee.client, InvalidClient):
                reason = "Invalid Engine Configuration. "
                for e  in [me.client, ce.client, se.client, ee.client]:
                    if isinstance(e, InvalidClient):
                        reason += e.reason + ". "

                return InvalidConfig(self.config, reason)
            else:
                logger.debug("Valid Configuration")
                return ValidConfig(self.config, me, ce, se, ee)

        else:
            return InvalidConfig(self.config, f"Invalid config schema. Reason: {schema.reason}")



