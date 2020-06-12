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

cli.add_command(config)
cli.add_command(run)
cli.add_command(operator)
cli.add_command(register)
cli.add_command(workflow)

if __name__ == "__cli__":
    cli()

