import click
import shutil
import os
from os import path
from pathlib import Path
from typing import Optional

from configurations import Config
from parameters import Parameters
from util.json_schema import validate_schema
from util.yaml import parse_yaml
from util.logger import logger
import operators as Operators
from util.printer import  banner
from util.environment import MasonEnvironment

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
    logger.set_level(log_level)
    env = MasonEnvironment()

    if not path.exists(env.mason_home):
        logger.info(f"Creating MASON_HOME at {env.mason_home}")
        os.mkdir(env.mason_home)
    if not path.exists(env.operator_home):
        logger.info(f"Creating OPERATOR_HOME at {env.operator_home}")
        os.mkdir(env.operator_home)
        Path(env.operator_home + "__init__.py").touch()

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
                        logger.error(f"Error validating client schema: {name}")
                        valid = False
                        break
        if valid:
            logger.info()
            logger.info(f"Valid Configuration. Saving config {config_file} to {env.config_home}")
            logger.info()
            shutil.copyfile(config_file, env.config_home)
            return Config(env)
        else:
            logger.error()
            logger.error(f"Invalid Config Schema: {config_file}")
    else:
        if path.exists(env.config_home):
            return Config(env)
        else:
            logger.error()
            logger.error("Configuration not found.")
            logger.error("First pass configuration:  \"mason config <config_file_path>\"")


@main.command()
@click.argument("operator_file")
@click.option("-l", "--log_level", help="Log level for mason")
def register(operator_file: str, log_level: Optional[str] = None):
    env = MasonEnvironment()
    if path.exists(env.config_home):
        logger.set_level(log_level)
        validation = Operators.validate_operators(operator_file)
        if len(validation[1]) == 0:

            basename = path.basename(operator_file.rstrip("/"))
            pathname = env.operator_home + f"{basename}/"

            if not path.exists(pathname):
                logger.info(f"Registering operator(s) at {operator_file} to {pathname}")
                shutil.copytree(operator_file, pathname)
            else:
                logger.info(f"Operator \"{basename}\" already exists at {pathname}")
        else:
            logger.error(f"Invalid operator configurations found: {validation[1]}")

    else:
        logger.error("Configuration not found.  Run \"mason config\" first")


@main.command()
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Parameters as \",\" delimeted list var=value")
@click.option('-c', '--param_file', help="Parameters from yaml file path")
@click.option("-l", "--log_level", help="Log level for mason")
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, log_level: Optional[str] = None):
    env = MasonEnvironment()

    if path.exists(env.config_home):
        logger.set_level(log_level)
        config = Config(env)
        params = Parameters(parameters, param_file)
        Operators.run(config, params, cmd, subcmd)

    else:
        logger.info("Configuration not found.  Run \"mason config\" first")

@main.command()
def run():
    os.system('./scripts/run.sh')


if __name__ == "__main__":
    main()

