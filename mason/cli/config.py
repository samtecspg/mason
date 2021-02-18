from typing import Optional

import click

from mason.api.config import config as api_config

@click.command("config", short_help="Interact with mason configs")
@click.argument("config_id", required=True)
@click.option("-s", "--set_current", help="Sets config as current session config")
@click.option("-l", "--log_level", help="Log level for mason")

def config(config_id: str, set_current: bool = False, log_level: Optional[str] = None):
    """
    Sets mason config for session.  Alias for get:namespace=config_id, with added ability to set session config
    [CONFIG_ID] is a config_id 
    """
    api_config(config_id, set_current, log_level)
