import contextlib

from configurations.valid_config import ValidConfig
from test.support.mocks import get_patches
from util.logger import logger
from configurations.configurations import get_all
from definitions import from_root
import re
from util.uuid import uuid_regex
from dotenv import load_dotenv

from typing import List, Optional

from clients.response import Response
from configurations.configurations import get_config
from operators.operator import Operator
from operators import operators as Operators
from util.environment import MasonEnvironment

load_dotenv(from_root('/.env.example'))

def clean_uuid(s: str, subst: str = ''):
    return uuid_regex().sub(subst, s)

def ansi_escape(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def clean_string(s1: str):
    e = re.compile(r"(\s|\n|\t)")
    f1 = e.sub('', ansi_escape(s1))
    return f1

def run_tests(cmd: str, sub: str, do_mock: bool, log_level: str, configs: List[str], callable, *args, **kwargs):
    logger.set_level(log_level)
    env = get_env()
    if do_mock:
        patches = get_patches()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_test(env, cmd, sub, configs, callable)
    else:
        run_test(env, cmd, sub, configs, callable)


def run_test(env: MasonEnvironment, cmd: str, sub: str, configs: List[str], callable):
    response = Response()
    op: Optional[Operator] = Operators.get_operator(env, cmd, sub)

    if op:
        operator: Operator = op
        for config in configs:
            conf = get_config(env, config + ".yaml")
            if isinstance(conf, ValidConfig):
                callable(env, conf, operator)
            else:
                raise Exception(f"No matching valid configuration found for operator {op.namespace}, {op.command}. Reason {conf.reason}")

    else:
        raise Exception(f"Operator not found {cmd} {sub}")

def set_log_level(level: str = None):
    logger.set_level(level or "fatal", False)

def get_env(operator_home: str = "/examples/operators", config_home = "/examples/operators/table/test_configs/", operator_module: str = "examples.operators", workflow_home: str = "/examples/workflows", workflow_module="examples.workflows"):
    return MasonEnvironment(operator_home=from_root(operator_home), config_home=from_root(config_home), operator_module=operator_module, workflow_home=from_root(workflow_home), workflow_module=workflow_module)

def get_configs(env: MasonEnvironment):
    return get_all(env)
