
import click

from mason.cli.apply import apply
from mason.cli.get import get
from mason.cli.validate import validate
from mason.cli.run import run
from mason.cli.server import server
from mason.cli.config import config

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

cli.add_command(server)
cli.add_command(apply)
cli.add_command(get)
cli.add_command(run)
cli.add_command(validate)
cli.add_command(config)

if (__name__ == "__cli__") or (__name__ == "__main__"):
    cli()

