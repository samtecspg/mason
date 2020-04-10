
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
from engines import Engine
from clients.response import Response
import re
from util.uuid import uuid_regex

LOG_LEVEL = "fatal"
# LOG_LEVEL = "trace"
# MOCK = True
MOCK = False

def clean_uuid(s: str):
    return uuid_regex().sub('', s)

def ansi_escape(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def assert_multiline(s1: str, s2: str):
    e = re.compile(r"(\s|\n|\t)")
    f1 = e.sub('', ansi_escape(s1))
    f2 = e.sub('', s2)
    assert (f1 == f2)

def run_tests(cmd: str, sub: str, mock: bool, callable):
    set_log_level()
    env = get_env()
    response = Response()
    configs = get_configs(env)
    op: Optional[Operator] = Operators.get_operator(env, cmd, sub)

    if op:
        operator: Operator = op
        config,response = op.find_configuration(configs, response)
        if config:
            if mock or MOCK:
                get_mocks(config)
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

def get_mocks(config: Config):
    get_mock(config.metastore)
    get_mock(config.scheduler)
    get_mock(config.execution)
    get_mock(config.storage)

def get_mock(engine: Engine):
    client_name = engine.client_name
    if client_name:
        if client_name == "glue":
            logger.info("Mocking Glue Client")
            engine.set_underlying_client(GlueMock())
        elif client_name == "s3":
            logger.info("Mocking S3 Client")
            engine.set_underlying_client(S3Mock())
        elif client_name == "spark":
            # TODO: IMPLMENT THIS
            logger.info("Mocking Spark Client")
            engine.set_underlying_client(None)
        elif client_name == "invalid":
            pass
        else:
            raise Exception(f"Unmocked Client Implementation: {client_name}")


