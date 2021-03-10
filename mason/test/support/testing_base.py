import contextlib
import re
from dotenv import load_dotenv
from typing import List, Optional, Union

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources import base 
from mason.resources.malformed import MalformedResource
from mason.test.support.mocks import get_patches
from mason.util.logger import logger
from mason.definitions import from_root
from mason.util.uuid import uuid_regex
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
    op: Union[Operator, Workflow, MalformedResource]
    res = base.Resources(env)
    
    if workflow:
        op = res.get_workflow(namespace, command)
    else:
        op = res.get_operator(namespace, command)
        
    if not isinstance(op, MalformedResource) and (op != None):
        for config in configs:
            conf = res.get_config(config)
            # invalid = res.get_bad()
            # for inv in invalid:
            #     print(f"Invalid Config: {inv.get_message()}")
            # conf = get_config(env, config)
            if isinstance(conf, Config):
                callable(env, conf, op)
            else:
                raise Exception(f"No matching valid configuration found for {namespace}:{command}.")
    else:
        raise Exception(f"No operator found: {namespace}:{command}")

def set_log_level(level: str = None):
    logger.set_level(level or "fatal", False)

def get_env(home: Optional[str] = None, validations: Optional[str] = None):
    return MasonEnvironment(from_root(home or "/examples/"), from_root(validations or "/validations/"))

