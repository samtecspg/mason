from os import path
from typing import Optional
import click


@click.argument("file")
@click.option("-f", "--force", help="Force rewrite definition", is_flag=True)
@click.option("-l", "--log_level", help="Log level for mason")
@click.command('register', short_help="Registers mason operators or workflows")
def register(file: str, force: bool = False, log_level: Optional[str] = None):
    """
    Registers mason operator or workflow located at [FILE].

    See examples/operators/ for examples of operators.
    See examples/workflows/ for examples of operators.

    Valid operators will contain an operator.yaml file which defines the compatible clients and parameters for the operator.
    Valid workflows will contain an workflow.yaml file which defines the compatible clients and parameters for the workflow and the workflow steps.
    """

    from mason.operators import operators
    from mason.util.environment import MasonEnvironment
    from mason.util.logger import logger
    from mason.workflows import workflows

    env = MasonEnvironment()
    if path.exists(file):
        if path.exists(env.config_home):
            logger.set_level(log_level)
            
            valid_ops, invalid_ops = operators.list_operators(file, True)
            valid_workflows, invalid_workflows = workflows.parse_workflows(file, True)

            if len(valid_ops) > 0:
                for operator in valid_ops:
                    operator.register_to(env.operator_home, force)
                    
            if len(invalid_ops) > 0:
                for error in invalid_ops:
                    logger.error(f"Invalid operator configurations found: {error.reason}")

            if len(valid_workflows) > 0:
                for workflow in valid_workflows:
                    workflow.register_to(env.workflow_home, force)

            if len(invalid_workflows) > 0:
                for workflow_error in invalid_workflows:
                    logger.error(f"Invalid workflow configurations found: {workflow_error.reason}")

        else:
            logger.error("Configuration not found.  Run \"mason config\" first")
    else:
        logger.error(f"File not found {file}")


