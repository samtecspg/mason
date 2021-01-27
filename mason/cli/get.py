from typing import Optional
import click
from mason.validations.validate import validate_files, validate_configs, validate_operators, validate_workflows


@click.command("apply", short_help="Apply mason yaml file")
@click.option('-f', 'file', required=True, help="Mason yaml specification or directory")
@click.option("-l", "--log_level", help="Log level for mason")
def get(file: str, resource_type: str, log_level: Optional[str] = None):
    """
    Gets mason resource.  Available resources: [config, operator, workflow]
    Configs are identified via their config id
    Workflows and operators are identified by their namespace and name
    
    Example:
        mason get config 1
        mason get 
    """

    from mason.util.environment import MasonEnvironment, initialize_environment

    env = MasonEnvironment()
    initialize_environment(env)
    
    if resource_type in ["config", "configs"]:
        validate_configs
    validate_operators
    validate_workflows

    print("HERE")


