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
from util import environment as env
from sys import path
from util.json_schema import validate_schema
from typing import List
from util.logger import Logger

class Operators:

    def __init__(self, logger: Logger):
        self.logger = logger

    def run(self, config: Config, parameters: Parameters, cmd: Optional[str] = None, subcmd: Optional[str] = None, debug: Optional[bool] = False):

        #  TODO: Allow single step commands without subcommands
        response = Response()

        if cmd == None:
            self.tabulate_operators()
        elif subcmd == None:
            self.tabulate_operators(cmd)
        else:
            path.append(env.MASON_HOME)
            mod = import_module(f'registered_operators.{cmd}.{subcmd}')
            op = self.get_operator(cmd, subcmd)
            resp = self.validate_parameters(op, parameters, response)
            banner("Operator Response")
            if len(resp.errors) == 0:
                resp = mod.run(config, parameters, resp)  # type: ignore
            resp.formatted(debug)

    def validate_operators(self, operator_file: str):
        configs: List[dict] = []
        errors: List[dict] = []

        for r, d, f in os.walk(operator_file):
            for file in f:
                if '.yaml' in file:
                    file_path = os.path.join(r, file)
                    if file == "operator.yaml":
                        config = parse_yaml(file_path)
                        schema = "operators/schema.json"
                        if validate_schema(config, schema):
                            self.logger.debug(f"Valid Operator Definition {file_path}")
                            configs.append(config)
                        else:
                            self.logger.error(f"Invalid Operator Definition: {file_path}")
                            errors.append(config)

        return configs, errors


    def validate_parameters(self, op: Optional[dict], parameters: Parameters, response: Response):
        required_params = set((op or {}).get("parameters", {}).get("required"))
        provided_params = set(parameters.parsed_parameters.keys())
        sym_diff = required_params.symmetric_difference(provided_params)
        intersection = required_params.intersection(provided_params)
        parameters.add_valid(list(intersection))

        print()
        validated = list(parameters.validated_parameters.keys())
        missing = list(sym_diff)
        banner(f"Parameters Validation:")
        if len(validated) > 0:
            print(f"Validated: {validated}")
        if len(missing) > 0:
            print(f"Missing: {missing}")
        print()

        if len(sym_diff) > 0:
            dp = ", ".join(list(sym_diff))
            response.add_error(f"Missing required parameters: {dp}")
        return response


    def list_operators(self, cmd: Optional[str] = None):
        path = env.OPERATOR_HOME

        configs = self.validate_operators(path)[0]
        grouped = dict((k, list(g)) for k, g in itertools.groupby(configs, key=itemgetter('namespace')))
        filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}

        return filtered

    def get_operator(self, cmd: Optional[str], subcmd: Optional[str]) -> Optional[dict]:
        try:
            ops = self.list_operators(cmd)[cmd]
            return list(filter(lambda x: x.get("command") == subcmd, ops))[0]
        except Exception as e:
            print(f"Could not find operator {cmd} {subcmd}")
            return None

    def tabulate_operators(self, cmd: Optional[str] = None):
        ops = self.list_operators(cmd)
        array = []
        for k in ops:
            for item in ops[k]:
                command = item.get("command")
                description = item.get("description")
                parameters = item.get("parameters")
                array.append([k, command, description, parameters])

        cmd_value = (cmd or "Operator")
        print()
        if len(array) > 0:
            banner(f"Available {cmd_value} Methods: {env.OPERATOR_HOME}")
            print()
            print(tabulate(array, headers=["namespace", "command", "description", "parameters"]))
        else:
            if cmd:
                print(f"Operator \"{cmd_value}\" not found.  List operators but running \"mason operator\"")
            else:
                print("No Operators Registered.  Register operators by running \"mason register\"")

