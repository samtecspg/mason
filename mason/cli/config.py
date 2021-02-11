from typing import Optional

import click

@click.command("config", short_help="Interact with mason configs")
@click.argument("config_id", required=False)
@click.option("-s", "--set_current", help="Sets config as current session config")
@click.option("-l", "--log_level", help="Log level for mason")

def config(config_id: Optional[str] = None, set_current: bool = False, log_level: Optional[str] = None):
    """
    Sets mason config for session.  Alias for get:namespace=config_id, with added ability to set session config
    [CONFIG_ID] is a config_id 
    """
    from mason.cli.cli_printer import CliPrinter
    from mason.api.get import get as api_get
    api_get("configs", config_id, None, log_level, None, CliPrinter())

