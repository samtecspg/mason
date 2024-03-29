from typing import Optional

import click

@click.command("validate", short_help="Validates and performs a dry run of a mason workflow or operator")
@click.argument("resource", required=True)
@click.argument("namespace", required=True)
@click.argument("command", required=True)
@click.option('-p', '--parameters', help="Load parameters from mason.parameters string of the format  <param1>:<value1>,<param2>:<value2>")
@click.option('-f', '--param_file', help="Parameters from yaml file path")
@click.option("-c", "--config_id", help="Specified config id for run")
@click.option("-l", "--log_level", help="Log level for mason")

def validate(resource: str, namespace: str, command: str, parameters: Optional[str] = None, param_file: Optional[str] = None, config_id: Optional[str] = None, log_level: Optional[str] = None):
    """
    Validates mason workflow or operator with specified configuration and parameters and performs a dry run
    Example:
        
    [RESOURCE] is the resource type, current supported: [config, operator, workflow] 
    [NAMESPACE] is a namespace string. See examples/ for reference implementations.
    [COMMAND] is a command string.  See examples/ for reference implementations.
    """
    from mason.api.run import run as api_run
    api_run(resource, namespace, command, parameters, param_file, config_id, log_level, dry_run=True)
