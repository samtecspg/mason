
from util.logger import logger
from util.environment import MasonEnvironment
from configurations import Config
from operators import operators as Operators
from operators.operator import Operator
from typing import Tuple, Optional
from test.support.mocks.clients.glue import GlueMock
from definitions import from_root

LOG_LEVEL = "error"

def before(cmd: str, sub: str) -> Tuple[Config, Optional[Operator]]:
    set_log_level()
    config = get_config()
    op = Operators.get_operator(config, cmd, sub)
    if config and op:
        o: Operator = op
        return (config, o)
    else:
        raise Exception(f"Operator not found {cmd} {sub}")

def set_log_level(level: str = None):
    logger.set_level(level or LOG_LEVEL, False)

def get_config():
    env = MasonEnvironment(operator_home= from_root("/examples/operators/"), config_home=from_root("/examples/operators/table/test_config.yaml"))
    return Config(env)

def get_mock(client: str):
    if client == "glue":
        return GlueMock
