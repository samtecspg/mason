import os
import sys

from tabulate import tabulate
from importlib import import_module

from operators.invalid_operator import InvalidOperator
from operators.namespaces.namespace import Namespace
from operators import namespaces as Namespaces

from operators.valid_operator import ValidOperator
from util.swagger import update_yaml_file
from typing import Optional, Union, Tuple
from configurations.valid_config import ValidConfig
from parameters import InputParameters
from clients.response import Response
from util.printer import banner
from sys import path
from util.json_schema import parse_schemas
from typing import List
from util.logger import logger
from util.json import print_json
from operators.operator import Operator
from util.environment import MasonEnvironment

def import_all(env: MasonEnvironment):
    path.append(env.mason_home)
    namespaces, invalid = list_namespaces(env)
    for namespace in namespaces:
        for op in namespace.operators:
            cmd = op.command
            import_module(f"{env.operator_module}.{namespace}.{cmd}")

def update_yaml(env: MasonEnvironment, base_swagger: str):
    update_yaml_file(base_swagger, env.operator_home)

def run(env: MasonEnvironment, config: ValidConfig, parameters: InputParameters, cmd: Optional[str] = None, subcmd: Optional[str] = None):
    sys.path.append(env.mason_home)

    #  TODO: Allow single step commands without subcommands
    response = Response()

    namespaces, invalid = list_namespaces(env, cmd)

    if len(invalid) > 0:
        for i in invalid:
            response.add_error(i.reason)

    if cmd and subcmd:
        namespace: str = cmd
        command: str = subcmd
        op = Namespaces.get(namespaces, namespace, command)

        if op:
            operator: Union[ValidOperator, InvalidOperator] = op.validate(config, parameters)
            response = operator.run(env, response)
        else:
            response.add_error(f"Operator {namespace} {command} not found.  Check operators with 'mason operator'")

        banner("Operator Response")
        print_json(response.formatted())
        return response
    else:
        tabulate_operators(env, namespaces, cmd)


def from_config(config: dict, source_path: Optional[str] = None):
    namespace = config.get("namespace")
    command = config.get("command")
    description = config.get("description", "")
    parameters = config.get("parameters", {})
    supported_configurations = config.get("supported_configurations", [])
    if namespace and command:
        return Operator(namespace, command, description, parameters, supported_configurations, source_path)
    else:
        None

def list_operators(operator_file: str) -> Tuple[List[Operator], List[InvalidOperator]]:

    valid, errors = parse_schemas(operator_file, "operator", Operator)
    invalid = list(map(lambda e: InvalidOperator(e), errors))

    return valid, invalid

def get_operator(env: MasonEnvironment, namespace: str, command: str) -> Optional[Operator]:
    return Namespaces.get(list_namespaces(env, namespace)[0], namespace, command)

def list_namespaces(env: MasonEnvironment, cmd: Optional[str] = None) -> Tuple[List[Namespace], List[InvalidOperator]]:
    path = env.operator_home
    ops, invalid = list_operators(path)

    namespaces = Namespaces.from_ops(ops)
    filtered = Namespaces.filter(namespaces, cmd)
    return filtered, invalid

def tabulate_operators(env: MasonEnvironment, namespaces: List[Namespace], cmd: Optional[str] = None):
    array = []
    ops = Namespaces.get_all(namespaces)

    for op in ops:
        array.append([op.namespace, op.command, op.description, op.parameters.to_dict()])

    if len(array) > 0:
        cmd_value = cmd or "Operator"
        banner(f"Available {cmd_value} Methods: {env.operator_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description", "parameters"]))
    else:
        if cmd:
            logger.error(f"Operator namespace \"{cmd}\" not found.  List operators but running \"mason operator\"")
        else:
            logger.error("No Operators Registered.  Register operators by running \"mason register\"")


