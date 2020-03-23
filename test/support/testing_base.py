
from util.logger import logger
from util.environment import MasonEnvironment
from configurations import Config, get_all
from operators import operators as Operators
from operators.operator import Operator
from typing import Tuple, Optional
from test.support.mocks.clients.glue import GlueMock
from test.support.mocks.clients.s3 import S3Mock
from definitions import from_root
from operators.supported_engines import SupportedEngineSet
from clients.response import Response
from functools import partial

from unittest.mock import patch

LOG_LEVEL = "trace"

def with_mock(engine_type: str, client: Optional[str]):
    if client:
        with patch(f'engines.{engine_type}.{str.capitalize(engine_type)}Engine.get_client', new=get_mock(client).method):
            return callable
    else:
        return callable

def with_mocks(metastore_client: Optional[str], storage_client: Optional[str], execution_client: Optional[str], scheduler_client: Optional[str], callable):
    mm = partial(with_mock("metastore", metastore_client))
    sm = partial(with_mock("storage", storage_client))
    em = partial(with_mock("execution", execution_client))
    cm = partial(with_mock("scheduler", scheduler_client))
    return sm(mm(em(cm(callable))))

def run_tests(cmd: str, sub: str, callable):
    set_log_level("fatal")
    config = get_config()
    op: Optional[Operator] = Operators.get_operator(config, cmd, sub)
    if config and op:
        operator: Operator = op
        for sc in op.supported_configurations:
             with_mocks(sc.metastore, sc.storage, sc.execution, sc.scheduler, callable)
             callable(op, config)
    else:
        raise Exception(f"Operator not found {cmd} {sub}")


def set_log_level(level: str = None):
    logger.set_level(level or LOG_LEVEL, False)

def get_config(operator_home: str = "/examples/operators", config_home = "/examples/operators/table/test_configs/", operator_module: str = "examples.operators", test: bool = False):
    env = MasonEnvironment(operator_home=from_root(operator_home), config_home=from_root(config_home), operator_module=operator_module, test=test)
    return get_all(env)[0]

def get_mock(client: str):
    if client == "glue":
        return GlueMock
    elif client == "s3":
        return S3Mock


