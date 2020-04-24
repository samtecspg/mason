from test.support import mocks as Mocks
from util.logger import logger
from util.environment import MasonEnvironment
from configurations import get_all, get_config
from operators import operators as Operators
from operators.operator import Operator
from typing import Optional, List
from definitions import from_root
from clients.response import Response
import re
from util.uuid import uuid_regex
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv # type: ignore

load_dotenv('.env.example')

def clean_uuid(s: str):
    return uuid_regex().sub('', s)

def ansi_escape(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def clean_string(s1: str):
    e = re.compile(r"(\s|\n|\t)")
    f1 = e.sub('', ansi_escape(s1))
    return f1


def mock(client: str):
    logger.debug(f"Mocking {client} client")
    return MagicMock(return_value=Mocks.get_client(client))


def run_tests(cmd: str, sub: str, do_mock: bool, log_level: str, configs: List[str], callable):
    logger.set_level(log_level)
    if do_mock:
        with patch('clients.glue.GlueClient.client', mock("glue")):
            with patch('clients.s3.S3Client.client', mock("s3")):
                with patch('clients.spark.SparkClient.client', mock("kubernetes")):
                    execute_tests(cmd,sub, configs, callable)
    else:
        execute_tests(cmd,sub, configs, callable)

def execute_tests(cmd: str, sub: str, configs: List[str], callable):
    set_log_level()
    env = get_env()
    response = Response()
    op: Optional[Operator] = Operators.get_operator(env, cmd, sub)

    if op:
        operator: Operator = op
        for config in configs:
            conf = get_config(env, config + ".yaml")
            if conf and conf.valid:
                callable(env, conf, operator)
            else:
                raise Exception(f"No matching valid configuration found for operator {op.cmd}, {op.subcommand}")

    else:
        raise Exception(f"Operator not found {cmd} {sub}")


def set_log_level(level: str = None):
    logger.set_level(level or "fatal", False)

def get_env(operator_home: str = "/examples/operators", config_home = "/examples/operators/table/test_configs/", operator_module: str = "examples.operators"):
    return MasonEnvironment(operator_home=from_root(operator_home), config_home=from_root(config_home), operator_module=operator_module)

def get_configs(env: MasonEnvironment):
    return get_all(env)

