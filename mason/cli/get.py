import click

from typing import Optional

from mason.resources.filter import Filter
from mason.resources.printer import Printer
from mason.validations.validate import validate_resource


@click.command("get", short_help="Get mason resource")
@click.argument("resource", required=True)
@click.argument("namespace", required=False)
@click.argument("command", required=False)
@click.option("-l", "--log_level", help="Log level for mason")
def get(resource: str, namespace: Optional[str] = None, command: Optional[str] = None, log_level: Optional[str] = None):
    """
    Gets mason resource.  Available resources: [config, operator, workflow]
    Configs are identified via their config id
    Workflows and operators are identified by their namespace and command
   
    [RESOURCE] is the resouce type, current supported: [config, operator, workflow] 
    [NAMESPACE] is a namespace string. For configs, NAMESPACE==ConfigId  See examples/ for reference implementations.
    [COMMAND] is a command string.  See examples/ for reference implementations.
    """

    from mason.util.environment import MasonEnvironment

    env = MasonEnvironment().initialize()
    printer, filter = Printer(), Filter()
    all = validate_resource(resource, env)
    filtered = filter.filter_resources(resource, all, namespace, command)
    printer.print_resource(resource, filtered, namespace)
