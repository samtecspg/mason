
import click

from mason.cli.apply import apply
from mason.cli.config import config
from mason.cli.operator import operator
from mason.cli.register import register
from mason.cli.server import run
from mason.cli.workflow import workflow

@click.group()
@click.version_option(prog_name="Mason", message='%(prog)s -v %(version)s')

def cli():
    """
    \b
    ___  ___
    |  \/  |
    | .  . | __ _ ___  ___  _ __
    | |\/| |/ _` / __|/ _ \| '_ \     
    | |  | | (_| \__ \ (_) | | | |
    \_|  |_/\__,_|___/\___/|_| |_|

    Mason Data Operator Framework 
    """

# cli.add_command(apply)
cli.add_command(run)
cli.add_command(operator)
cli.add_command(register)
cli.add_command(workflow)

if __name__ == "__cli__":
    cli()

