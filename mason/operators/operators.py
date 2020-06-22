import sys

from tabulate import tabulate
from importlib import import_module
from sys import path
from typing import List

from mason.operators import namespaces
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.namespaces.namespace import Namespace

from mason.operators.operator import Operator
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.operators.valid_operator import ValidOperator
from typing import Optional, Union, Tuple
from mason.configurations.valid_config import ValidConfig
from mason.parameters.input_parameters import InputParameters
from mason.clients.response import Response
from mason.util.list import sequence
from mason.util.swagger import update_yaml_file
from mason.util.environment import MasonEnvironment
from mason.util.printer import banner
from mason.util.json_schema import parse_schemas
from mason.util.logger import logger
from mason.util.json import print_json


def import_all(env: MasonEnvironment):
    path.append(env.mason_home)
    namespaces, invalid = list_namespaces(env)
    for namespace in namespaces:
        for op in namespace.operators:
            cmd = op.command
            import_module(f"{env.operator_module}.{namespace.namespace}.{cmd}")

def update_yaml(env: MasonEnvironment, base_swagger: str):
    update_yaml_file(base_swagger, [env.operator_home, env.workflow_home])

def run(env: MasonEnvironment, config: ValidConfig, parameters: InputParameters, cmd: Optional[str] = None, subcmd: Optional[str] = None) -> OperatorResponse:
    sys.path.append(env.mason_home)

    #  TODO: Allow single step commands without subcommands
    response = Response()

    ns, invalid = list_namespaces(env, cmd)

    if len(invalid) > 0:
        for i in invalid:
            response.add_error(i.reason)

    if cmd and subcmd:
        namespace: str = cmd
        command: str = subcmd
        op = namespaces.get(ns, namespace, command)

        if op:
            operator: Union[ValidOperator, InvalidOperator] = op.validate(config, parameters)
            operator_response: OperatorResponse = operator.run(env, response)
        else:
            response.add_error(f"Operator {namespace} {command} not found.  Check operators with 'mason operator'")
            operator_response = OperatorResponse(response)

        banner("Operator Response")
        print_json(operator_response.formatted())
    else:
        tabulate_operators(env, ns, cmd)
        operator_response = OperatorResponse(response)
        
    return operator_response


def from_config(config: dict, source_path: Optional[str] = None):
    namespace = config.get("namespace")
    command = config.get("command")
    description = config.get("description", "")
    parameters = config.get("parameters", {})
    supported_configurations = config.get("supported_configurations", [])
    if namespace and command:
        return Operator(namespace, command, description, parameters, supported_configurations, source_path)
    else:
        return None

def check_definition(operator: Operator) -> Union[Operator, InvalidOperator]:
    module = operator.module()
    if isinstance(module, OperatorDefinition):
        return operator
    else:
        return module

def list_operators(operator_file: str, validate_source: bool = False) -> Tuple[List[Operator], List[InvalidOperator]]:
    valid, errors = parse_schemas(operator_file, "operator", Operator)
    invalid = list(map(lambda e: InvalidOperator(e), errors))

    if validate_source:
        valid, invalid_def = sequence(list(map(lambda v: check_definition(v), valid)), Operator, InvalidOperator)
        invalid = invalid + invalid_def

    return valid, invalid

def get_operator(env: MasonEnvironment, namespace: str, command: str) -> Optional[Operator]:
    return namespaces.get(list_namespaces(env, namespace)[0], namespace, command)

def list_namespaces(env: MasonEnvironment, cmd: Optional[str] = None) -> Tuple[List[Namespace], List[InvalidOperator]]:
    operator_path = env.operator_home
    ops, invalid = list_operators(operator_path)

    ns = namespaces.from_ops(ops)
    filtered = namespaces.filter(ns, cmd)
    return filtered, invalid

def tabulate_operators(env: MasonEnvironment, ns: List[Namespace], cmd: Optional[str] = None):
    array = []
    ops = namespaces.get_all(ns)

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


