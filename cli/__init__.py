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
import operators.operators as Operators
from util.printer import banner
from util.environment import MasonEnvironment
from configurations import get_all
from definitions import from_root

@click.group()
def main():
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

@main.command("config", short_help="Configures mason clients and engines")
@click.argument('config_file', required=False)
@click.option("-l", "--log_level", help="Log level for mason")
def config(config_file: Optional[str] = None, log_level: Optional[str] = None):
    """
    Configures mason according to [CONFIG_FILE].

    [CONFIG_FILE] is a yaml file.  See examples/config/ for reference implementations.
    """

    logger.set_level(log_level)
    env = MasonEnvironment()

    if not path.exists(env.mason_home):
        banner(f"Creating MASON_HOME at {env.mason_home}")
        os.mkdir(env.mason_home)
    if not path.exists(env.operator_home):
        banner(f"Creating OPERATOR_HOME at {env.operator_home}")
        os.mkdir(env.operator_home)
        Path(env.operator_home + "__init__.py").touch()
    if not path.exists(env.config_home):
        banner(f"Creating CONFIG_HOME at {env.config_home}")
        os.mkdir(env.config_home)

    if config_file:
        # TODO: Interactive configuration
        banner("Config Validation")
        parsed = parse_yaml(config_file)
        valid = validate_schema(parsed, env.config_schema)
        if valid:
            clients: dict = parsed.get("clients")
            #  TODO:  Use json schema partials for this
            valid = True
            if clients and len(clients) > 0:
                for name, config in clients.items():
                    schema = from_root(f"/clients/{name}/schema.json")
                    if not validate_schema(config, schema):
                        logger.error(f"Error validating client schema: {name}")
                        valid = False
                        break
        if valid:
            logger.info()
            logger.info(f"Valid Configuration. Saving config {config_file} to {env.config_home}")
            logger.info()
            shutil.copyfile(config_file, env.config_home + os.path.basename(config_file))
            return Config(env, parsed)
        else:
            logger.error()
            logger.error(f"Invalid Config Schema: {config_file}")
    else:
        if path.exists(env.config_home):
            return get_all(env)
        else:
            logger.error()
            logger.error("Configuration not found.")
            logger.error("First pass configuration:  \"mason config <config_file_path>\"")


@click.argument("operator_file")
@click.option("-l", "--log_level", help="Log level for mason")
@main.command('register', short_help="Registers mason operator")
def register(operator_file: str, log_level: Optional[str] = None):
    """
    Registers mason operator using [OPERATOR_FILE].

    See examples/operators/table/ for examples of operators.

    Valid operators will contain an operator.yaml file which defines the compatible clients and parmeters for the operator.
    """

    env = MasonEnvironment()
    if path.exists(env.config_home):
        logger.set_level(log_level)

        validation = Operators.validate_operators(operator_file, True)
        if len(validation[1]) == 0:

            basename = path.basename(operator_file.rstrip("/"))
            pathname = env.operator_home + f"{basename}/"

            if not path.exists(pathname):
                logger.info(f"Registering operators at {operator_file} to {pathname}")
                shutil.copytree(operator_file, pathname)
            else:
                logger.error(f"Operator \"{basename}\" already exists at {pathname}")
        else:
            logger.error(f"Invalid operator configurations found: {validation[1]}")

    else:
        logger.error("Configuration not found.  Run \"mason config\" first")


@main.command("operator", short_help="Executes and lists mason operators")
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Load parameters from parameters string of the format  <param1>:<value1>,<param2>:<value2>")
@click.option('-f', '--param_file', help="Parameters from yaml file path")
@click.option("-l", "--log_level", help="Log level for mason")
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, log_level: Optional[str] = None):
    """
    Running without cmd or subcmd will list out all mason operators currently registered.
    Running without subcmd will list out all mason operators under the cmd namespace.
    Running with both cmd and subcmd will execute the operator or print out missing required parameters.
    """
    env = MasonEnvironment()

    if path.exists(env.config_home):
        logger.set_level(log_level)
        config = get_all(env)[0] # return first config for now
        params = Parameters(parameters, param_file)
        Operators.run(env, config, params, cmd, subcmd)

    else:
        logger.info("Configuration not found.  Run \"mason config\" first")

@main.command("run", short_help="Runs mason flask server on port 5000")
def run():
    """
    Will run mason flask server on port 5000.
    To view the mason swagger ui go to: http://localhost:5000/api/ui/
    """
    os.system('./scripts/run.sh')


if __name__ == "__main__":
    main()

