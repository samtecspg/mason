from os import path
from typing import Optional
import click

@click.argument("operator_file")
@click.option("-l", "--log_level", help="Log level for mason")
@click.command('register', short_help="Registers mason operator")
def register(operator_file: str, log_level: Optional[str] = None):
    """
    Registers mason operator using [OPERATOR_FILE].

    See examples/operators/ for examples of operators.

    Valid operators will contain an operator.yaml file which defines the compatible clients and parmeters for the operator.
    """

    from mason.operators import operators
    from mason.util.environment import MasonEnvironment
    from mason.util.logger import logger

    env = MasonEnvironment()
    if path.exists(operator_file):
        if path.exists(env.config_home):
            logger.set_level(log_level)
            ops, errors = operators.list_operators(operator_file)

            if len(ops) > 0:
                for operator in ops:
                    operator.register_to(env.operator_home)
            else:
                if len(errors) > 0:
                    for error in errors:
                        logger.error(f"Invalid operator configurations found: {error}")


        else:
            logger.error("Configuration not found.  Run \"mason config\" first")
    else:
        logger.error(f"File not found {operator_file}")


