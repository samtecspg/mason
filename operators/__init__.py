import os
import yaml
import itertools
from operator import itemgetter
from tabulate import tabulate
from importlib import import_module
from configurations import Config
from util.yaml import parse_yaml
from typing import Optional
from configurations import Config
from parameters import Parameters
from configurations.response import Response

class Operators:

    def run(self, config: Config, parameters: Parameters, cmd: Optional[str] = None, subcmd: Optional[str] = None):
        #  TODO: Allow single step commands without subcommands
        response = Response()

        if cmd == None:
            self.tabulate_operators()
        elif subcmd == None:
            self.tabulate_operators(cmd)
        else:
            mod = import_module(f'.', f"operators.{cmd}.{subcmd}")
            op = self.get_operator(cmd, subcmd)
            resp = self.validate_parameters(op, parameters, response)
            if len(resp.errors) == 0:
                resp = mod.run(config, parameters, resp)  # type: ignore
            print(resp.formatted())

    def validate_parameters(self, op: Optional[dict], parameters: Parameters, response: Response):
        required_params = set((op or {}).get("parameters", {}).get("required"))
        provided_params = set(parameters.parsed_parameters.keys())
        sym_diff = required_params.symmetric_difference(provided_params)
        intersection = required_params.intersection(provided_params)
        parameters.add_valid(list(intersection))

        print()
        validated = list(parameters.validated_parameters.keys())
        missing = list(sym_diff)
        print(f"Parameters Validation:")
        print(f"Validated: {validated}")
        print(f"Missing: {missing}")
        print()

        if len(sym_diff) > 0:
            dp = ", ".join(list(sym_diff))
            response.add_error(f"Missing required parameters: {dp}")
        return response


    def list_operators(self, cmd: Optional[str] = None):
        path = os.getcwd() + "/operators/"
        configs = []

        #  TODO: validate operator.yaml structure using json schema
        for r, d, f in os.walk(path):
            for file in f:
                if '.yaml' in file:
                    file_path = os.path.join(r, file)
                    if file == "operator.yaml":
                        config = parse_yaml(file_path)
                        configs.append(config)
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
        print(f"Available {cmd_value} Methods:")
        print()
        print(tabulate(array, headers=["command", "subcommand", "description", "parameters"]))

