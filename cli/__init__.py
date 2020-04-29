import click
import shutil
import os
from os import path
from typing import Optional

from parameters import Parameters
from util.logger import logger
import operators.operators as Operators
from util.environment import MasonEnvironment, initialize_environment
from configurations import get_current_config
from configurations.actions import run_configuration_actions
from dotenv import load_dotenv #type: ignore

load_dotenv()

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
@click.option("-s", "--set_current", help="Set current config to config id")
def config(config_file: Optional[str] = None, set_current: Optional[str] = None, log_level: Optional[str] = None):
    """
    Configures mason according to [CONFIG_FILE].

    [CONFIG_FILE] is a yaml file.  See examples/config/ for reference implementations.
    """

    env = MasonEnvironment()
    initialize_environment(env)
    run_configuration_actions(env, config_file=config_file, set_current=set_current, log_level=log_level)

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
    config = get_current_config(env, "debug")  # return first config for now

    logger.set_level(log_level or "info")

    if config:
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

