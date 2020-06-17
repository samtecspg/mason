import contextlib
import re
from dotenv import load_dotenv
from typing import List, Optional, Union

from mason.workflows import workflows
from mason.configurations.valid_config import ValidConfig
from mason.operators import operators
from mason.test.support.mocks import get_patches
from mason.util.logger import logger
from mason.configurations.configurations import get_all, get_config
from mason.definitions import from_root
from mason.util.uuid import uuid_regex
from mason.clients.response import Response
from mason.operators.operator import Operator
from mason.util.environment import MasonEnvironment
from mason.workflows.workflow import Workflow

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
    workflow = kwargs.get("workflow") or False
    if do_mock:
        patches = get_patches()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_test(env, cmd, sub, configs, workflow, callable)
    else:
        run_test(env, cmd, sub, configs, workflow, callable)


def run_test(env: MasonEnvironment, cmd: str, sub: str, configs: List[str], workflow: bool, callable):
    response = Response()
    op: Union[Optional[Operator], Optional[Workflow]] = None

    if workflow:
        op = workflows.get_workflow(env, cmd, sub)
        types = "Workflow"
    else:
        op = operators.get_operator(env.operator_home, cmd, sub)
        types = "Operator"
    if op:
        for config in configs:
            conf = get_config(from_root("/examples/operators/table/test_configs/") + config + ".yaml")
            if isinstance(conf, ValidConfig):
                callable(env, conf, op)
            else:
                raise Exception(f"No matching valid configuration found for {op.namespace}:{op.command}. Reason: {conf.reason}")

    else:
        raise Exception(f"{types} not found {cmd} {sub}")

def set_log_level(level: str = None):
    logger.set_level(level or "fatal", False)

def get_env(operator_home: str = "/examples/operators", config_home = "/examples/operators/table/test_configs/", operator_module: str = "mason.examples.operators", workflow_home: str = "/examples/workflows", workflow_module="examples.workflows"):
    return MasonEnvironment(operator_home=from_root(operator_home), config_home=from_root(config_home), operator_module=operator_module, workflow_home=from_root(workflow_home), workflow_module=workflow_module)

def get_configs(env: MasonEnvironment):
    valid, invalid = get_all(env)
    return list(valid.values())
