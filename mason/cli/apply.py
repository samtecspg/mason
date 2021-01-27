
from typing import Optional
import click
from mason.validations.validate import validate_files


@click.command("apply", short_help="Apply mason yaml file")
@click.option('-f', 'file', required=True, help="Mason yaml specification or directory")
@click.option("-l", "--log_level", help="Log level for mason")

def apply(file: str, log_level: Optional[str] = None):
    """
    Applies mason yaml file with unspecified use (config, operator, workflow) 

    [FILE] is a yaml file.  See examples/ for reference implementations.
    """

    from mason.util.environment import MasonEnvironment, initialize_environment

    env = MasonEnvironment()
    initialize_environment(env)
    
    objects = validate_files(file, env)
    print("HERE")
    

