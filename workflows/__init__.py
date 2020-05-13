from tabulate import tabulate

from configurations.valid_config import ValidConfig
from typing import Optional, Union, Tuple
from clients.response import Response
from parameters import InputParameters
from util.list import split_type, get
from util.printer import banner
from sys import path
from util.json_schema import parse_schemas
from typing import List
from util.logger import logger
from util.json import print_json
from util.environment import MasonEnvironment
from workflows.invalid_workflow import InvalidWorkflow
from workflows.valid_workflow import ValidWorkflow
from workflows.workflow import Workflow


def run(env: MasonEnvironment, config: ValidConfig, parameters: InputParameters, cmd: Optional[str] = None, subcmd: Optional[str] = None, deploy: bool = False, run: bool = False):
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
            validated = wf.validate(env, config, parameters)
            if isinstance(validated, ValidWorkflow):
                response = validated.run(env, response, deploy, run)
            else:
                response.add_error(f"Invalid Workflow: {validated.reason}")
        else:
            if not response.errored():
                response.add_error(f"Workflow {cmd} {subcmd} not found.  Check workflows with 'mason workflow'")

        banner("Workflow Response")
        print_json(response.formatted())
    return response


def parse_workflows(workflow_file: str) -> Tuple[List[Workflow], List[InvalidWorkflow]]:
    workflows, errors = parse_schemas(workflow_file, "workflow", Workflow)

    validated: List[Union[Workflow, InvalidWorkflow]] = list(map(lambda w: w.validate_config(), workflows))
    workflows, invalid = split_type(validated)
    schema_errors = list(map(lambda e: InvalidWorkflow("Invalid Workflow Schema: " + e), errors))
    invalid_workflows = schema_errors + invalid

    return workflows, invalid_workflows

def register_workflows(workflow_file: str, env: MasonEnvironment):
    valid_workflows, invalid_workflows = parse_workflows(workflow_file, env)

    for i in invalid_workflows:
        logger.error(f"Invalid Workflow Schema Definition {i.reason}")

    for w in valid_workflows:
        logger.info(f"Valid Workflow Definition: {workflow_file}")
        w.register_to(env.workflow_home)

def list_workflows(env: MasonEnvironment, namespace: Optional[str] = None) -> List[Workflow]:
    valid, invalid = parse_workflows(env.workflow_home)
    return [v for v in valid if (v.namespace == namespace or namespace == None)]

def get_workflow(env: MasonEnvironment, namespace: Optional[str], command: Optional[str]) -> Optional[Workflow]:
    workflows = list_workflows(env, namespace)
    return get([w for w in workflows if (w.command == command)], 0)

def tabulate_workflows(env: MasonEnvironment, cmd: Optional[str] = None):
    workflows = list_workflows(env, cmd)
    array = []
    for w in workflows:
        array.append([w.namespace, w.command, w.description or ""])

    cmd_value = (cmd or "Workflow")
    logger.info()
    if len(array) > 0:
        banner(f"Available {cmd_value} Methods: {env.operator_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description"]))
    else:
        if cmd:
            logger.error(f"Workflow \"{cmd_value}\" not found.  List workflows but running \"mason workflow\"")
        else:
            logger.error("No Workflows Registered.  Register worfklows by running \"mason workflow register\"")


