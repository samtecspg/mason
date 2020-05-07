import os
from tabulate import tabulate
from importlib import import_module
from util.yaml import parse_yaml
from typing import Optional
from configurations import Config
from parameters import Parameters
from clients.response import Response
from util.printer import banner
from sys import path
from util.json_schema import validate_schema, parse_schemas
from typing import List
from util.logger import logger
from util.json import print_json
import yaml
from operators.operator import Operator
from typing import Dict
from util.environment import MasonEnvironment

def import_all(env: MasonEnvironment, config: Config):
    path.append(env.mason_home)
    operators = list_operators(env)
    for namespace, ops in operators.items():
        for op in ops:
            cmd = op.command
            import_module(f"{env.operator_module}.{namespace}.{cmd}")

def update_yaml(env: MasonEnvironment, base_swagger: str):
    swagger_file = "api/swagger.yml"
    parsed_swagger = parse_yaml(base_swagger) or {}
    paths: dict = parsed_swagger["paths"]

    for r, d, f in os.walk(env.operator_home):
        for file in f:
            if '.yml' in file or '.yaml' in file:
                file_path = os.path.join(r, file)
                if file == "swagger.yml" or file == "swagger.yaml":
                    file_parsed = parse_yaml(file_path) or {}

                    parsed_paths = file_parsed.get('paths') or {}
                    if len(parsed_paths) > 0:
                        paths.update(parsed_paths)

    parsed_swagger['paths'] = paths
    with open(swagger_file, 'w+') as file: # type: ignore
        yaml.dump(parsed_swagger, file) # type: ignore


def run(env: MasonEnvironment, config: Config, parameters: Parameters, cmd: Optional[str] = None, subcmd: Optional[str] = None):
    #  TODO: Allow single step commands without subcommands
    response = Response()

    if cmd == None:
        tabulate_operators(env)
    elif subcmd == None:
        tabulate_operators(env, cmd)
    else:
        path.append(env.mason_home)
        op: Optional[Operator] = get_operator(env, cmd, subcmd)

        if op:
            response = op.run(env, config, parameters, response)
        else:
            if not response.errored():
                response.add_error(f"Operator {cmd} {subcmd} not found.  Check operators with 'mason operator'")

        banner("Operator Response")
        print_json(response.formatted())
        return response

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

def validate_operators(operator_file: str, print_validation: bool = False):

    operators, errors = parse_schemas(operator_file, "operator", Operator)
    for error in errors:
        logger.error(error)

    return operators, errors


def list_operators(env: MasonEnvironment, cmd: Optional[str] = None) -> Dict[str, List[Operator]]:
    path = env.operator_home
    operators = validate_operators(path)[0]
    grouped: Dict[str, List[Operator]] = {}

    for operator in operators:
        ops = grouped.get(operator.namespace) or []
        ops.append(operator)
        grouped[operator.namespace] = ops

    filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}

    return filtered

def get_operator(env: MasonEnvironment, cmd: Optional[str], subcmd: Optional[str]) -> Optional[Operator]:
    if cmd and subcmd:
        ops: List[Operator] = list_operators(env, cmd).get(cmd) or []
        filtered = list(filter(lambda x: x.command == subcmd, ops))
        if len(filtered) == 0:
            return None
        else:
            return filtered[0]
    else:
        return None

def tabulate_operators(env: MasonEnvironment, cmd: Optional[str] = None):
    ops = list_operators(env)
    array = []
    if cmd:
        for item in ops[cmd]:
            array.append([cmd, item.command, item.description, item.parameters])
    else:
        for k in ops:
            for item in ops[k]:
                array.append([k, item.command, item.description, item.parameters])

    cmd_value = (cmd or "Operator")
    logger.info()
    if len(array) > 0:
        banner(f"Available {cmd_value} Methods: {env.operator_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description", "parameters"]))
    else:
        if cmd:
            logger.error(f"Operator \"{cmd_value}\" not found.  List operators but running \"mason operator\"")
        else:
            logger.error("No Operators Registered.  Register operators by running \"mason register\"")


