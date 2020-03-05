import click
import shutil
import os
from os import path
from pathlib import Path
from operators import Operators
from typing import Optional

from configurations import Config
from parameters import Parameters
from util import environment as env
from util.json_schema import validate_schema
from util.printer import banner
from util.yaml import parse_yaml
from util.logger import Logger, LogLevel

@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass

@main.command()
@click.argument('config_file', required=False)
@click.option("-l", "--log_level", help="Log level for mason")
def config(config_file: Optional[str] = None, log_level: Optional[str] = None):
    logger = Logger(LogLevel(log_level))
    if not path.exists(env.MASON_HOME):
        print(f"Creating MASON_HOME at {env.MASON_HOME}")
        os.mkdir(env.MASON_HOME)
    if not path.exists(env.OPERATOR_HOME):
        print(f"Creating OPERATOR_HOME at {env.OPERATOR_HOME}")
        os.mkdir(env.OPERATOR_HOME)
        Path(env.OPERATOR_HOME + "__init__.py").touch()

    if config_file:
        # TODO: Interactive configuration
        banner("Config Validation")
        parsed = parse_yaml(config_file)
        valid = validate_schema(parsed, "configurations/schema.json")
        if valid:
            clients: dict = parsed.get("clients")
            #  TODO:  Use json schema partials for this
            valid = True
            if clients and len(clients) > 0:
                for name, config in clients.items():
                    schema = f"clients/{name}/schema.json"
                    if not validate_schema(config, schema):
                        print(f"Error validating client schema: {name}")
                        valid = False
                        break
        if valid:
            print()
            print(f"Valid Configuration. Saving config {config_file} to {env.CONFIG_HOME}")
            print()
            shutil.copyfile(config_file, env.CONFIG_HOME)
            return Config()
        else:
            print()
            print(f"Invalid Config Schema: {config_file}")
    else:
        if path.exists(env.CONFIG_HOME):
            return Config()
        else:
            print()
            print("Configuration not found.")
            print("First pass configuration:  \"mason config <config_file_path>\"")

@main.command()
@click.argument("operator_file")
@click.option("-l", "--log_level", help="Log level for mason")
def register(operator_file: str, log_level: Optional[str] = None):
    logger = Logger(LogLevel(log_level))
    if path.exists(env.CONFIG_HOME):

        validation = Operators(logger).validate_operators(operator_file)
        if len(validation[1]) == 0:

            #  TODO: Assumes it is a path
            basename = path.basename(operator_file)
            pathname = env.OPERATOR_HOME + f"{basename}/"

            if not path.exists(pathname):
                print(f"Registering operator(s) at {operator_file} to {pathname}")
                shutil.copytree(operator_file, pathname)
            else:
                print(f"Operator \"{basename}\" already exists at {pathname}")
        else:
            print(f"Invalid operator configurations found: {validation[1]}")
    else:
        print("Configuration not found.  Run \"mason config\" first")

@main.command()
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Parameters as \",\" delimeted list var=value")
@click.option('-c', '--param_file', help="Parameters from yaml file path")
@click.option('-d', '--debug', help="Return client responses", is_flag=True)
@click.option("-l", "--log_level", help="Log level for mason")
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, debug: bool = False, log_level: Optional[str] = None):
    logger = Logger(LogLevel(log_level))
    params = Parameters(parameters, param_file)
    if path.exists(env.CONFIG_HOME):
        configuration = Config()
        Operators(logger).run(configuration, params, cmd, subcmd, debug)
    else:
        logger.info("Configuration not found.  Run \"mason config\" first")


if __name__ == "__main__":
    main()
