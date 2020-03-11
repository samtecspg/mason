
from util.logger import logger
from util.environment import MasonEnvironment
from configurations import Config
import operators as Operators
from configurations.valid_operator import ValidOperator
from typing import Tuple, Optional
from test.support.mocks.clients.glue import GlueMock
from definitions import from_root

LOG_LEVEL = "error"

def before(cmd: str, sub: str) -> Tuple[Config, Optional[ValidOperator]]:
    set_log_level()
    config = get_config()
    op = Operators.get_operator(config, cmd, sub)
    if config and op:
        o: ValidOperator = op
        return (config, o)
    else:
        raise Exception(f"Operator not found {cmd} {sub}")

def set_log_level():
    logger.set_level(LOG_LEVEL, False)

def get_config():
    env = MasonEnvironment(operator_home= from_root("/examples/operators/"), config_home=from_root("/examples/operators/table/test_config.yaml"))
    return Config(env)

def get_mock(client: str):
    if client == "glue":
        return GlueMock
