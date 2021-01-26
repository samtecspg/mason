from typing import Optional
import click

@click.command("config", short_help="Configure mason session")
@click.argument('config_file', required=False)
@click.option("-l", "--log_level", help="Log level for mason")
@click.option("-s", "--set_current", help="Set current config to config id")
def config(config_file: Optional[str] = None, set_current: Optional[str] = None, log_level: Optional[str] = None):
    """
    Configures mason according to [CONFIG_FILE].

    [CONFIG_FILE] is a yaml file.  See examples/config/ for reference implementations.
    """

    from mason.configurations.actions import run_configuration_actions
    from mason.util.environment import MasonEnvironment, initialize_environment

    env = MasonEnvironment()
    initialize_environment(env)
    run_configuration_actions(env, config_file=config_file, set_current=set_current, log_level=log_level)
