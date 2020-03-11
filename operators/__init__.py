import os
import itertools
from operator import itemgetter
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
from configurations.valid_operator import ValidOperator


def import_all(config: Config):
    path.append(config.env.mason_home)
    operators = list_operators(config)
    for namespace, ops in operators.items():
        for op in ops:
            cmd = op.get("command")
            import_module(f"registered_operators.{namespace}.{cmd}")

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
    with open(swagger_file, 'w+') as file:
        yaml.dump(parsed_swagger, file)


def run(config: Config, parameters: Parameters, cmd: Optional[str] = None, subcmd: Optional[str] = None):
    #  TODO: Allow single step commands without subcommands
    response = Response()

    if cmd == None:
        tabulate_operators(config)
    elif subcmd == None:
        tabulate_operators(config, cmd)
    else:
        path.append(config.env.mason_home)
        op = get_operator(config, cmd, subcmd)
        if op:
            response = parameters.validate(op, response)
            if not response.errored():
                mod = import_module(f'registered_operators.{cmd}.{subcmd}')
                response = mod.run(config, parameters, response) # type: ignore

        else:
            response.add_error(f"Operator {cmd} {subcmd} not found.  Check operators with 'mason operator'")

        banner("Operator Response")
        print_json(response.formatted())
        return response


# TODO: FIX
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def validate_operators(operator_file: str):
    configs: List[dict] = []
    errors: List[dict] = []

    for r, d, f in os.walk(operator_file):
        for file in f:
            if '.yaml' in file:
                file_path = os.path.join(r, file)
                if file == "operator.yaml":
                    config = parse_yaml(file_path)
                    schema = ROOT_DIR + "/schema.json"
                    if validate_schema(config, schema):
                        logger.debug(f"Valid Operator Definition {file_path}")
                        configs.append(config)
                    else:
                        logger.error(f"Invalid Operator Definition: {file_path}")
                        errors.append(config)

    return configs, errors



def list_operators(config: Config, cmd: Optional[str] = None) -> dict:
    path = config.env.operator_home

    configs = validate_operators(path)[0]
    grouped = dict((k, list(g)) for k, g in itertools.groupby(configs, key=itemgetter('namespace')))
    filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}

    return filtered

def get_operator(config: Config, cmd: Optional[str], subcmd: Optional[str]) -> Optional[ValidOperator]:
    try:
        ops: dict = list_operators(config, cmd)[cmd]
        op: dict = list(filter(lambda x: x.get("command") == subcmd, ops))[0]
        ns = op.get("namespace")
        command = op.get("command")
        required_parameters = op.get("parameters", {}).get("required", [])
        supported_clients = op.get("supported_clients", [])
        if ns and command:
            return ValidOperator(ns, command, required_parameters, supported_clients)
        else:
            return None
    except Exception as e:
        logger.error(f"Could not find operator {cmd} {subcmd}")
        return None

def tabulate_operators(config: Config, cmd: Optional[str] = None):
    ops = list_operators(config)
    array = []
    for k in ops:
        for item in ops[k]:
            command = item.get("command")
            description = item.get("description")
            parameters = item.get("parameters")
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


