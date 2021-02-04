from typing import Optional

import click

from mason.resources.filter import Filter
from mason.util.logger import logger
from mason.validations.validate import validate_resource


@click.command("validate", short_help="Validates and performs a dry run of a mason workflow or operator")
@click.argument("resource", required=True)
@click.argument("namespace", required=True)
@click.argument("command", required=True)
@click.option('-p', '--parameters', help="Load parameters from mason.parameters string of the format  <param1>:<value1>,<param2>:<value2>")
@click.option('-f', '--param_file', help="Parameters from yaml file path")
@click.option("-c", "--config_id", help="Specified config id for operator runs")
@click.option("-l", "--log_level", help="Log level for mason")

def validate(resource: str, namespace: str, command: str, parameters: Optional[str] = None, param_file: Optional[str] = None, config_id: Optional[str] = None, log_level: Optional[str] = None):
    """
    Validates mason workflow or operator with passed parameters and performs a dry run
    Example:
        
    """
    from mason.util.environment import MasonEnvironment

    env = MasonEnvironment().initialize()
    all = validate_resource(resource, env)
    configs = validate_resource()
    resource = Filter().get_resource(resource, all, namespace, command)
    if resource:
        resource.validate()
    else:
        logger.error(f"Resource not found: {resource}:{namespace}:{command}")
        
