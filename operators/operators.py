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
from util.json_schema import validate_schema
from typing import List
from util.logger import logger
from util.json import print_json
import yaml
from operators.valid_operator import Operator
from definitions import from_root
from typing import Dict

def import_all(config: Config):
    path.append(config.env.mason_home)
    operators = list_operators(config)
    for namespace, ops in operators.items():
        for op in ops:
            cmd = op.subcommand
            import_module(f"{config.env.operator_module}.{namespace}.{cmd}")


def update_yaml(config: Config, base_swagger: str):
    swagger_file = "api/swagger.yml"
    parsed_swagger = parse_yaml(base_swagger) or {}
    paths: dict = {}

    for r, d, f in os.walk(config.env.operator_home):
        for file in f:
            if '.yml' in file:
                file_path = os.path.join(r, file)
                if file == "swagger.yml":
                    file_parsed = parse_yaml(file_path) or {}

                    parsed_paths = file_parsed.get('paths') or {}
                    if len(parsed_paths) > 0:
                        paths.update(parsed_paths)

    parsed_swagger['paths'] = paths
    with open(swagger_file, 'w+') as file: # type: ignore
        yaml.dump(parsed_swagger, file) # type: ignore


def run(config: Config, parameters: Parameters, cmd: Optional[str] = None, subcmd: Optional[str] = None):
    #  TODO: Allow single step commands without subcommands
    response = Response()

    if cmd == None:
        tabulate_operators(config)
    elif subcmd == None:
        tabulate_operators(config, cmd)
    else:
        path.append(config.env.mason_home)
        op: Optional[Operator] = get_operator(config, cmd, subcmd)
        if op:
            response = parameters.validate(op, response)
            if not response.errored():
                mod = import_module(f'{config.env.operator_module}.{cmd}.{subcmd}')
                response = mod.run(config, parameters, response) # type: ignore

        else:
            response.add_error(f"Operator {cmd} {subcmd} not found.  Check operators with 'mason operator'")

        banner("Operator Response")
        print_json(response.formatted())
        return response

def from_config(config: dict):
    namespace = config.get("namespace")
    command = config.get("command")
    description = config.get("description", "")
    parameters = config.get("parameters", {})
    supported_clients = config.get("supported_clients", [])
    if namespace and command:
        return Operator(namespace, command, description, parameters, supported_clients)
    else:
        None

def validate_operators(operator_file: str):
    operators: List[Operator] = []
    errors: List[dict] = []

    for r, d, f in os.walk(operator_file):
        for file in f:
            if '.yaml' in file:
                file_path = os.path.join(r, file)
                if file == "operator.yaml":
                    config = parse_yaml(file_path)
                    schema = from_root("/operators/schema.json")
                    if validate_schema(config, schema):
                        logger.debug(f"Valid Operator Definition {file_path}")
                        operator = from_config(config)
                        if operator:
                            operators.append(operator)
                    else:
                        logger.error(f"Invalid Operator Definition: {file_path}")
                        errors.append(config)

    return operators, errors


def list_operators(config: Config, cmd: Optional[str] = None) -> Dict[str, List[Operator]]:
    path = config.env.operator_home
    operators = validate_operators(path)[0]
    grouped: Dict[str, List[Operator]] = {}
    for operator in operators:
        ops = grouped.get(operator.cmd) or []
        ops.append(operator)
        grouped[operator.cmd] = ops

    filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}


    return filtered

def get_operator(config: Config, cmd: Optional[str], subcmd: Optional[str]) -> Optional[Operator]:
    if cmd and subcmd:
        ops: List[Operator] = list_operators(config, cmd).get(cmd) or []
        filtered = list(filter(lambda x: x.subcommand == subcmd, ops))
        if len(filtered) == 0:
            return None
        else:
            return filtered[0]
    else:
        return None

def tabulate_operators(config: Config, cmd: Optional[str] = None):
    ops = list_operators(config)
    array = []
    for k in ops:
        for item in ops[k]:
            command = item.subcommand
            description = item.description
            parameters = item.parameters
            array.append([k, command, description, parameters])

    cmd_value = (cmd or "Operator")
    logger.info()
    if len(array) > 0:
        banner(f"Available {cmd_value} Methods: {config.env.operator_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description", "parameters"]))
    else:
        if cmd:
            logger.error(f"Operator \"{cmd_value}\" not found.  List operators but running \"mason operator\"")
        else:
            logger.error("No Operators Registered.  Register operators by running \"mason register\"")


