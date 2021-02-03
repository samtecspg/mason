from tabulate import tabulate

from typing import Optional, Tuple, Union
from mason.clients.response import Response
from mason.configurations.config import Config
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.list import get, sequence
from mason.util.printer import banner
from sys import path
from mason.util.json_schema import parse_schemas
from typing import List
from mason.util.logger import logger
from mason.util.json import print_json
from mason.util.environment import MasonEnvironment
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow
from mason.workflows.workflow import Workflow
from mason.workflows.workflow_definition import WorkflowDefinition

def run(env: MasonEnvironment, config: Config, parameters: WorkflowParameters, cmd: Optional[str] = None, subcmd: Optional[str] = None, deploy: bool = False, run: bool = False, schedule_name: Optional[str] = None, output_path: Optional[str] = None):
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
                response = validated.run(env, response, not deploy, run, schedule_name, output_path)
            else:
                response.add_error(f"Invalid Workflow: {validated.reason}")
        else:
            response.add_error(f"Workflow {cmd} {subcmd} not found.  Check workflows with 'mason workflow'")

        banner("Workflow Response")
        print_json(response.formatted())
    return response

def parse_workflows(workflow_file: str, check_def: bool = False) -> Tuple[List[Workflow], List[InvalidWorkflow]]:
    workflows, errors = parse_schemas(workflow_file, "workflow", Workflow)
    schema_errors = list(map(lambda e: InvalidWorkflow("Invalid Workflow Schema: " + e), errors))
    invalid_workflows = schema_errors

    if check_def:
        workflows, invalid_def = sequence(list(map(lambda v: check_definition(v),workflows)), Workflow, InvalidWorkflow)
        invalid_workflows = invalid_def + invalid_workflows

    return workflows, invalid_workflows

def check_definition(workflow: Workflow) -> Union[Workflow, InvalidWorkflow]:
    module = workflow.module()
    if isinstance(module, WorkflowDefinition):
        return workflow
    else:
        return module

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
        banner(f"Available {cmd_value} Methods: {env.workflow_home}")
        logger.info()
        logger.info(tabulate(array, headers=["namespace", "command", "description"]))
    else:
        if cmd:
            logger.error(f"Workflow \"{cmd_value}\" not found.  List workflows but running \"mason workflow\"")
        else:
            logger.error("No Workflows Registered.  Register worfklows by running \"mason workflow register\"")


