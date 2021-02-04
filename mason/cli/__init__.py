
import click

from mason.cli.apply import apply
from mason.cli.get import get
from mason.cli.server import server

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
cli.add_command(server)
cli.add_command(apply)
cli.add_command(get)
# cli.add_command(operator)
# cli.add_command(register)
# cli.add_command(workflow)

if __name__ == "__cli__":
    cli()

