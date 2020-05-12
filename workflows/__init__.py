from tabulate import tabulate

from configurations.valid_config import ValidConfig
from operators.operators import list_namespaces
from operators.operator import Operator
from typing import Optional
from clients.response import Response
from util.printer import banner
from sys import path
from util.json_schema import parse_schemas
from typing import List
from util.logger import logger
from util.json import print_json
from typing import Dict
from util.environment import MasonEnvironment
from workflows.workflow import Workflow
from operators import namespaces as Namespaces


def run(env: MasonEnvironment, config: ValidConfig, cmd: Optional[str] = None, subcmd: Optional[str] = None, deploy: bool = False):
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
            response = wf.run(env, config, response, deploy)
        else:
            if not response.errored():
                response.add_error(f"Workflow {cmd} {subcmd} not found.  Check workflows with 'mason workflow'")

        banner("Workflow Response")
        print_json(response.formatted())
    return response


def validate_workflows(workflow_file: str, env: MasonEnvironment) -> List[Workflow]:
    namespaces, invalid = list_namespaces(env)
    operators: List[Operator] = Namespaces.get_all(namespaces)
    workflows, errors = parse_schemas(workflow_file, "workflow", Workflow)
    valid_workflows: List[Workflow] = []

    for workflow in workflows:

        validation = workflow.validate(operators)
        if isinstance(validation, bool) and validation == True:
            logger.info(f"Valid Workflow Definition")
            valid_workflows.append(workflow)
        else:
            error = f"Invalid Workflow Definition {workflow.source_path}.  Reason:  {validation}"
            errors.append(error)

    for error in errors:
        logger.error(error)

    return valid_workflows


def list_workflows(env: MasonEnvironment, cmd: Optional[str] = None) -> Dict[str, List[Workflow]]:
    path = env.workflow_home
    workflows = validate_workflows(path, env)
    grouped: Dict[str, List[Workflow]] = {}

    for workflow in workflows:
        wfs = grouped.get(workflow.namespace) or []
        wfs.append(workflow)
        grouped[workflow.namespace] = wfs

    filtered = {k: v for k, v in grouped.items() if (k == cmd) or (cmd == None)}

    return filtered

def get_workflow(env: MasonEnvironment, cmd: Optional[str], subcmd: Optional[str]) -> Optional[Workflow]:
    if cmd and subcmd:
        wfs: List[Workflow] = list_workflows(env, cmd).get(cmd) or []
        filtered = list(filter(lambda x: x.command == subcmd, wfs))
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
            array.append([item.namespace, item.command, item.description or ""])
    else:
        for k in ops:
            for item in ops[k]:
                array.append([item.namespace, item.command, item.description or ""])

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


