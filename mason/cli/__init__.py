import click
from mason.cli.config import config
from mason.cli.operator import operator
from mason.cli.register import register
from mason.cli.run import run
from mason.cli.workflow import workflow


@click.group()
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

if __name__ == 'cli':
    cli.add_command(Config.config)
    cli.add_command(Run.run)
    cli.add_command(Operator.operator)
    cli.add_command(Register.register)
    cli.add_command(Workflow.workflow)
    cli()
