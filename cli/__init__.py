import click
import shutil
import os
from os import path
from operators import Operators
from typing import Optional

from configurations import Config
from parameters import Parameters
from util import environment as env

@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass

@main.command()
@click.argument('config_file', required=False)
def config(config_file: Optional[str] = None):
    if config_file:
        try:
            os.mkdir(env.MASON_HOME)
            print(f"Creating MASON_HOME at {env.MASON_HOME}")
            os.mkdir(env.OPERATOR_HOME)
            print(f"Creating OPERATOR_HOME at {env.OPERATOR_HOME}")
        except FileExistsError as e:
            print(f"Using MASON_HOME at {env.MASON_HOME}")

        # TODO: Validate config
        # TODO: Interactive configuration
        print(f"Using config {config_file}.  Saving to {env.CONFIG_HOME}")
        shutil.copyfile(config_file, env.CONFIG_HOME)
    else:
        if path.exists(env.CONFIG_HOME):
            Config()
        else:
            print("Usage:  mason config <config_file_path>")

@main.command()
@click.argument("operator_file")
def register(operator_file: str):
    # TODO: Validate Operator using json_schema
    config()
    # shutil.copyfile()

@main.command()
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Parameters as \",\" delimeted list var=value")
@click.option('-c', '--param_file', help="Parameters from yaml file path")
@click.option('-d', '--debug', help="Return client responses", is_flag=True)
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, debug: bool = False):
    params = Parameters(parameters, param_file)
    config = Config()

    Operators().run(config, params, cmd, subcmd, debug)

if __name__ == "__main__":
    main()
