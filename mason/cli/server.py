import click

@click.command("server", short_help="Runs mason flask server on port 5000")
def run():
    """
    Will run mason flask server on port 5000.
    To view the mason swagger ui go to: http://localhost:5000/api/ui/
    """
    from mason.server import MasonServer
    MasonServer().run()

