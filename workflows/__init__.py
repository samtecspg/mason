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
from operators.operator import Operator
from definitions import from_root
from typing import Dict
from util.environment import MasonEnvironment
from workflows.workflow import Workflow


def import_all(env: MasonEnvironment, config: Config):
    path.append(env.mason_home)
    workflows = list_workflows(env)
    for namespace, ops in workflows.items():
        for op in ops:
            cmd = op.subcommand
            import_module(f"{env.workflow_module}.{namespace}.{cmd}")

def run(env: MasonEnvironment, config: Config, cmd: Optional[str] = None, subcmd: Optional[str] = None):
    #  TODO: Allow single step commands without subcommands
    response = Response()

    if cmd == None:
        tabulate_workflows(env)
    elif subcmd == None:
        tabulate_workflows(env, cmd)
    else:
        path.append(env.mason_home)
        wf: Optional[Workflow] = get_workflow(env, cmd, subcmd)

        if wf:
            response = workflow.run(env, config, response)
        else:
            if not response.errored():
                response.add_error(f"Workflow {cmd} {subcmd} not found.  Check workflows with 'mason workflow'")

        banner("Workflow Response")
        print_json(response.formatted())
        return response

def from_config(config: dict, source_path: Optional[str] = None) -> Optional[Workflow]:
    namespace = config.get("namespace")
    command = config.get("command")
    description = config.get("description")
    if namespace and command:
        return Workflow(namespace, command, source_path=source_path)
    else:
        None

def validate_workflows(workflow_file: str, print_validation: bool = False):
    workflows: List[Workflow] = []
    errors: List[dict] = []
    for r, d, f in os.walk(workflow_file):
        for file in f:
            if '.yaml' in file:
                file_path = os.path.join(r, file)
                if file == "workflow.yaml":
                    config = parse_yaml(file_path)
                    schema = from_root("/workflows/schema.json")
                    if validate_schema(config, schema):
                        if print_validation:
                            logger.info(f"Valid Workflow Definition {file_path}")
                        workflow = from_config(config, file_path)
                        if workflow:
                            workflows.append(workflow)
                    else:
                        logger.error(f"Invalid Workflow Definition: {file_path}")

    return workflows, errors


def list_workflows(env: MasonEnvironment, cmd: Optional[str] = None) -> Dict[str, List[Workflow]]:
    path = env.workflow_home
    workflows = validate_workflows(path)[0]
    grouped: Dict[str, List[Workflow]] = {}

    for workflow in workflows:
        wfs = grouped.get(workflow.command) or []
        wfs.append(workflow)
        grouped[workflow.command] = wfs

    filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}

    return filtered

def get_workflow(env: MasonEnvironment, cmd: Optional[str], subcmd: Optional[str]) -> Optional[Operator]:
    if cmd and subcmd:
        wfs: List[Workflow] = list_workflows(env, cmd).get(cmd) or []
        filtered = list(filter(lambda x: x.subcommand == subcmd, wfs))
        if len(filtered) == 0:
            return None
        else:
            return filtered[0]
    else:
        return None

def tabulate_workflows(env: MasonEnvironment, cmd: Optional[str] = None):
    ops = list_workflows(env)
    array = []
    if cmd:
        for item in ops[cmd]:
            array.append([cmd, item.subcommand, item.description, item.parameters])
    else:
        for k in ops:
            for item in ops[k]:
                array.append([k, item.subcommand, item.description, item.parameters])

    cmd_value = (cmd or "Workflow")
    logger.info()
    if len(array) > 0:
        banner(f"Available {cmd_value} Methods: {env.operator_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description"]))
    else:
        if cmd:
            logger.error(f"Operator \"{cmd_value}\" not found.  List workflows but running \"mason workflow\"")
        else:
            logger.error("No Workflows Registered.  Register worfklows by running \"mason workflow -r\"")


