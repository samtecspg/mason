
from util.logger import logger
from util.environment import MasonEnvironment
from configurations import get_all
from operators import operators as Operators
from operators.operator import Operator
from typing import Optional
from definitions import from_root
from test.support.mocks.clients.glue import GlueMock
from test.support.mocks.clients.s3 import S3Mock
from configurations import Config
from clients.response import Response
from typing import List

# LOG_LEVEL = "trace"
LOG_LEVEL = "fatal"
MOCK = True
# MOCK = False

def run_tests(cmd: str, sub: str, callable):
        set_log_level()
        env = get_env()
        response = Response()
        configs = get_configs(env)
        op: Optional[Operator] = Operators.get_operator(env, cmd, sub)

        if op:
            operator: Operator = op
            if MOCK:
                get_mocks(configs)
            config,response = op.find_configuration(configs, response)
            if config:
                callable(env, config, operator)
            else:
                raise Exception(f"No matching configuration found for operator {op.cmd}, {op.subcommand}")

        else:
            raise Exception(f"Operator not found {cmd} {sub}")


def set_log_level(level: str = None):
    logger.set_level(level or LOG_LEVEL, False)

def get_env(operator_home: str = "/examples/operators", config_home = "/examples/operators/table/test_configs/", operator_module: str = "examples.operators"):
    return MasonEnvironment(operator_home=from_root(operator_home), config_home=from_root(config_home), operator_module=operator_module)

def get_configs(env: MasonEnvironment):
    # env.config_schema = from_root("/test/support/schemas/config.json")
    return get_all(env)

def get_mocks(configs: List[Config]):
    # TODO: Clean this up, not parallel safe
    for config in configs:
        if config.metastore and config.metastore.client:
            config.metastore.client.client.client = get_mock(config.metastore.client_name)
        if config.storage and config.storage.client:
            config.storage.client.client.client = get_mock(config.storage.client_name)
        if config.execution and config.execution.client:
            config.execution.client.client.client = get_mock(config.execution.client_name)
        if config.scheduler and config.scheduler.client:
            config.scheduler.client.client.client = get_mock(config.scheduler.client_name)

def get_mock(client: Optional[str]):
    if client == "glue":
        logger.info("Mocking Glue Client")
        return GlueMock()
    elif client == "s3":
        logger.info("Mocking S3 Client")
        return S3Mock()
    else:
        return None


