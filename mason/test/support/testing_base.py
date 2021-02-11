import contextlib
import re
from dotenv import load_dotenv
from typing import List, Optional, Union

from typistry.validators.base import sequence

from mason.resources.base import get_operator, get_workflow, get_config 
from mason.test.support.mocks import get_patches
from mason.util.logger import logger
from mason.definitions import from_root
from mason.util.uuid import uuid_regex
from mason.clients.response import Response
from mason.operators.operator import Operator
from mason.util.environment import MasonEnvironment
from mason.workflows.workflow import Workflow
from mason.configurations.config import Config

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

def run_tests(namespace: str, command: str, do_mock: bool, log_level: str, configs: List[str], callable, *args, **kwargs):
    logger.set_level(log_level)
    env = get_env()
    load_dotenv(from_root("/../.env.example"))
    workflow = kwargs.get("workflow") or False
    if do_mock:
        patches = get_patches()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_test(env, namespace, command, configs, workflow, callable)
    else:
        run_test(env, namespace, command, configs, workflow, callable)


def run_test(env: MasonEnvironment, namespace: str, command: str, configs: List[str], workflow: bool, callable):
    return Response()
    # response = Response()
    # op: Union[Optional[Operator], Optional[Workflow]] = None
    # 
    # if workflow:
    #     op = get_workflow(env, namespace, command)
    #     types = "Workflow"
    # else:
    #     op = get_operator(env, namespace, command)
    #     types = "Operator"
    # if op:
    #     for config in configs:
    #         configs, invalid = sequence(get_configs(env), Config)
    #         for inv in invalid:
    #             print(f"Invalid Config: {inv.message}")
    #         conf: Optional[Config] = get_config_by_id(env, config)
    #         if isinstance(conf, Config):
    #             callable(env, conf, op)
    #         else:
    #             raise Exception(f"No matching valid configuration found for {op.namespace}:{op.command}.")
    # 
    # else:
    #     raise Exception(f"{types} not found {namespace} {command}")

def set_log_level(level: str = None):
    logger.set_level(level or "fatal", False)

def get_env(home: Optional[str] = None, validations: Optional[str] = None):
    return MasonEnvironment(from_root(home or "/examples/"), from_root(validations or "/validations/"))

# def get_configs(env: MasonEnvironment):
#     valid, invalid = get_all(env)
#     return list(valid.values())
