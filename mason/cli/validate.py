from typing import Optional

import click

from mason.util.environment import initialize_environment

@click.command("validate", short_help="Validates and performs a dry run of a mason workflow or operator")
@click.option('-f', 'file', required=True, help="Mason yaml specification or directory")
@click.option("-l", "--log_level", help="Log level for mason")
def validate(file: str, resource_type: str, log_level: Optional[str] = None):
    """
    Validates mason workflow or operator with passed parameters and performs a dry run
    Example:
        
    """

    from mason.util.environment import MasonEnvironment

    env = MasonEnvironment()
    initialize_environment(env)

    if resource_type in ["config", "configs"]:
        validate_configs
    validate_operators
    validate_workflows

    print("HERE")
