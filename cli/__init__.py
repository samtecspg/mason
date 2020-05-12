import click
import os
from os import path
from typing import Optional

from parameters import InputParameters
from util.logger import logger
from operators import operators as Operators
import workflows as Workflows
from util.environment import MasonEnvironment, initialize_environment
from configurations.configurations import get_current_config
from configurations.actions import run_configuration_actions
from dotenv import load_dotenv

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

    See examples/operators/ for examples of operators.

    Valid operators will contain an operator.yaml file which defines the compatible clients and parmeters for the operator.
    """

    env = MasonEnvironment()
    if path.exists(env.config_home):
        logger.set_level(log_level)
        operators, errors = Operators.list_operators(operator_file)

        if len(operators) > 0:
            for operator in operators:
                operator.register_to(env.operator_home)
        else:
            if len(errors) > 0:
                for error in errors:
                    logger.error(f"Invalid operator configurations found: {error}")


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
    config = get_current_config(env, "debug")

    logger.set_level(log_level or "info")

    if config:
        params = InputParameters(parameters, param_file)
        Operators.run(env, config, params, cmd, subcmd)
    else:
        logger.info("Configuration not found.  Run \"mason config\" first")

@main.command("workflow", short_help="Registers, lists and executes mason workflows")
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option("-l", "--log_level", help="Log level for mason")
@click.option("-d", "--deploy", help="Deploy specified workflow", is_flag=True)
def workflow(cmd: Optional[str] = None, subcmd: Optional[str] = None, log_level: Optional[str] = None, deploy: bool = False):
    """
    Running without cmd or subcmd will list out all mason workflows currently registered.
    Running without subcmd will list out all mason workflows under the cmd namespace.
    Running with both cmd and subcmd will execute the workflow or print out missing required parameters.
    Running with 'register' registers workflow from specified <workflow_file>, workflow_file must contain a valid workflow.yaml
    """
    env = MasonEnvironment()
    config = get_current_config(env, "debug")
    logger.set_level(log_level)

    if config:
        if cmd == "register":
            register_file = subcmd
            if register_file:
                workflows = Workflows.validate_workflows(register_file, env)
            else:
                logger.error("No file path provided to register operator")
            if len(workflows) > 0:
                for workflow in workflows:
                    workflow.register_to(env.workflow_home)
        else:
            Workflows.run(env, config, cmd, subcmd, not deploy)


    else:
        logger.error("Configuration not found.  Run \"mason config\" first")


@main.command("run", short_help="Runs mason flask server on port 5000")
def run():
    """
    Will run mason flask server on port 5000.
    To view the mason swagger ui go to: http://localhost:5000/api/ui/
    """
    os.system('./scripts/run.sh')


if __name__ == "__main__":
    main()

