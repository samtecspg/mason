
from typing import Union
from typing import List
from typing import Optional
import click

from mason.configurations.config import Config
from mason.operators.operator import Operator

from mason.validations.validate import validate_all
from typistry.protos.invalid_object import InvalidObject

from mason.workflows.workflow import Workflow

@click.command("apply", short_help="Apply mason yaml file")
@click.option('-f', 'file', required=True, help="Mason yaml specification or directory")
@click.option("-l", "--log_level", help="Log level for mason")

def apply(file: str, log_level: Optional[str] = None):
    """
    Applies mason yaml (Config, Operator, Workflow)
    Coming soon:  WorkflowRun, OperatorRun, OperatorDryRun, WorkflowDryRun

    [FILE] is a yaml file.  See examples/ for reference implementations.
    """

    from mason.util.environment import MasonEnvironment

    env = MasonEnvironment().initialize()
    all: List[Union[Operator, Workflow, Config, InvalidObject]] = validate_all(env, file)
    results = list(map(lambda i: i.save(env.state_store), all))
    return results

