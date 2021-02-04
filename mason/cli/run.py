import click

from typing import Optional, List, Union

from typistry.protos.invalid_object import InvalidObject

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.util.environment import MasonEnvironment
from mason.validations.validate import validate_resource
from mason.workflows.workflow import Workflow


@click.command("run", short_help="Runs mason workflow or operator")
@click.argument("resource", required=True)
@click.argument("namespace", required=False)
@click.argument("command", required=False)
@click.option("-l", "--log_level", help="Log level for mason")
def run(resource: str, namespace: Optional[str] = None, command: Optional[str] = None, log_level: Optional[str] = None):
    """
    """
    env = MasonEnvironment().initialize()
    all = validate_resource(resource, env)
    print("HERE")

    
    

