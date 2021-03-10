import click
from typing import Optional

@click.command("get", short_help="Get mason resource (operator, workflow, config)")
@click.argument("resource", required=False)
@click.argument("namespace", required=False)
@click.argument("command", required=False)
@click.option("-l", "--log_level", help="Log level for mason")
def get(resource: Optional[str], namespace: Optional[str] = None, command: Optional[str] = None, log_level: Optional[str] = None):
    """
    Gets mason resource.  Available resources: [config, operator, workflow]
    Configs are identified via their config id
    Workflows and operators are identified by their namespace and command
   
    [RESOURCE] is the resouce type, current supported: [config, operator, workflow] 
    [NAMESPACE] is a namespace string. For configs, NAMESPACE==config_id  See examples/ for reference implementations.
    [COMMAND] is a command string.  See examples/ for reference implementations.
    """
    from mason.cli.cli_printer import CliPrinter
    from mason.api.get import get as api_get
    api_get(resource, namespace, command, log_level, None, CliPrinter())

