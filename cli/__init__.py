import click
import shutil
import os
from os import path
from pathlib import Path
from operator_utils import Operators
from typing import Optional

from configurations import Config
from parameters import Parameters
from util import environment as env
from sys import path as spath

@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass

@main.command()
@click.argument('config_file', required=False)
def config(config_file: Optional[str] = None):
    if not path.exists(env.MASON_HOME):
        print(f"Creating MASON_HOME at {env.MASON_HOME}")
        os.mkdir(env.MASON_HOME)
    if not path.exists(env.OPERATOR_HOME):
        print(f"Creating OPERATOR_HOME at {env.OPERATOR_HOME}")
        os.mkdir(env.OPERATOR_HOME)
        Path(env.OPERATOR_HOME + "__init__.py").touch()

    if config_file:
        # TODO: Validate config using json_schema
        # TODO: Interactive configuration
        print()
        print(f"Using config {config_file}.  Saving to {env.CONFIG_HOME}")
        shutil.copyfile(config_file, env.CONFIG_HOME)
        return Config()
    else:
        if path.exists(env.CONFIG_HOME):
            return Config()
        else:
            print()
            print("Configuration not found.")
            print("First pass configuration:  \"mason config <config_file_path>\"")

@main.command()
@click.argument("operator_file")
def register(operator_file: str):
    if path.exists(env.CONFIG_HOME):
        # TODO: Validate Operator using json_schema

        #  Assumes it is a path
        basename = path.basename(operator_file)
        pathname = env.OPERATOR_HOME + f"{basename}/"

        if not path.exists(pathname):
            print(f"Registering operator at {operator_file} to {pathname}")
            shutil.copytree(operator_file, pathname)
        else:
            print(f"Operator \"{basename}\" already exists at {pathname}")
    else:
        print("Configuration not found.  Run \"mason config\" first")

@main.command()
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Parameters as \",\" delimeted list var=value")
@click.option('-c', '--param_file', help="Parameters from yaml file path")
@click.option('-d', '--debug', help="Return client responses", is_flag=True)
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, debug: bool = False):
    params = Parameters(parameters, param_file)
    if path.exists(env.CONFIG_HOME):
        configuration = Config()
        Operators().run(configuration, params, cmd, subcmd, debug)
    else:
        print("Configuration not found.  Run \"mason config\" first")


if __name__ == "__main__":
    main()
